# -*- coding: utf-8 -*-
# @Time   : 19-3-26 上午10:02
# @Author : huziying
# @File   : roles.py

from rest_framework.permissions import BasePermission

"""
10代表app首页是否能看到 治疗仪管理 的权限
首页海滨医疗入口权限 10 （部门）,app首页权限管理拉人但是没分配角色 10
超级管理员 1 ——> admin
普通管理员 common
医生      doctor
"""


class SuperAdminPermission(BasePermission):
    """超级管理员权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        role = request.redis_cache["role"]
        if role == "admin":
            return True
        else:
            return False


class CommonAdminPermission(BasePermission):
    """普通管理员权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        role = request.redis_cache["role"]
        # print("role=", role)
        if role == "common" or role == "admin":
            return True
        else:
            return False


class DoctorPermission(BasePermission):
    """医生权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        role = request.redis_cache["role"]
        if role == "doctor":
            return True
        else:
            return False


class AllPermission(BasePermission):
    """超级管理员/普通管理员/医生的权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        role = request.redis_cache["role"]
        if role == "admin" or role == "common" or role == "doctor":
            return True
        else:
            return False
