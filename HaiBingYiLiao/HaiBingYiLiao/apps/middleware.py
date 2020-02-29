# -*- coding: utf-8 -*-
# @Time   : 19-3-19 上午10:59
# @Author : huziying
# @File   : middleware.py

import time
import jwt
from django.conf import settings
from django.http import HttpResponse
from django.utils.deprecation import MiddlewareMixin
from django_redis import get_redis_connection
from django.db import connection
from rest_framework import status

from .constants import REDIS_CACHE


def _redis_pool_number():
    """输出redis连接池数量"""
    r = get_redis_connection("redis")  # Use the name you have defined for Redis in settings.CACHES
    connection_pool = r.connection_pool
    print("Created connections so far: %d" % connection_pool._created_connections)


class JwtTokenMiddleware(MiddlewareMixin):
    def process_request(self, request):
        # print(request.path)
        token = request.META.get("HTTP_AUTHORIZATION")
        if token:
            try:
                token = token.split(" ")[-1]
                # print(token)
                payload = jwt.decode(token, key=settings.JWT_SECRET_KEY, verify=True)
                if "username" in payload and "exp" in payload:
                    # print("payload=", payload)
                    REDIS_CACHE["username"] = payload["username"]
                    request.redis_cache = REDIS_CACHE
                    # print("request.redis_cache=", request.redis_cache)
                else:
                    raise jwt.InvalidTokenError
            except jwt.ExpiredSignatureError:
                return HttpResponse("jwt token expired", status=status.HTTP_401_UNAUTHORIZED)
            except jwt.InvalidTokenError:
                return HttpResponse("Invalid jwt token", status=status.HTTP_401_UNAUTHORIZED)
        else:
            return HttpResponse("lack of jwt token", status=status.HTTP_401_UNAUTHORIZED)

    def process_response(self, request, response):
        return response


class RedisMiddleware(MiddlewareMixin):
    """Redis读取缓存, hash类型
    key: "13212345678"
    field: value
    {"factory_id": "QtfjtzpNcM9DuGgR6e"},
    {"permission": "3,4,5,6,7,8"}
    """

    def process_request(self, request):
        phone = request.redis_cache["username"]
        conn = get_redis_connection("default")
        # print(phone), print(conn.hvals(phone))
        if phone.isdigit():
            factory_id = conn.hget(phone, "factory_id")
            permission = conn.hget(phone, "permission")
            role = conn.hget(phone, "role")

            if not factory_id or not permission or not role:
                # print("middleware------>", "factory_id=", factory_id, "permission=", permission)
                cursor = connection.cursor()
                pl = conn.pipeline()
                cursor.execute("select rights, factory from hb_roles where phone = '%s';" % phone)
                result = cursor.fetchone()
                # print("result=", result)
                if result:
                    role, factory_id = result[0], result[1]
                    pl.hset(phone, "role", role)
                    pl.hset(phone, "factory_id", factory_id)
                    pl.execute()

                cursor.execute("select rights from factory_users where factory = 'hbyl' and phone = '%s';" % phone)
                result2 = cursor.fetchone()
                # print("result2", result2)
                if result2:
                    permission = result2[0]
                    conn.hset(phone, "permission", ",".join(permission))

            request.redis_cache["factory_id"] = factory_id
            request.redis_cache["permission"] = permission
            request.redis_cache["role"] = role
        else:
            return None

    def process_response(self, request, response):
        return response
