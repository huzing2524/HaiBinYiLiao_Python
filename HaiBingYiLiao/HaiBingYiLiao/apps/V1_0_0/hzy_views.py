import json
import re
import arrow
import isoweek
import jwt
from django.conf import settings
import logging
import time
import datetime

from django_redis import get_redis_connection
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, AliOss, month_timestamp, someday_timestamp, UtilsRabbitmq, generate_uuid
from constants import ROW
from permissions import SuperAdminPermission, DoctorPermission, CommonAdminPermission, AllPermission

logger = logging.getLogger('django')


# Create your views here.
class GenerateToken(APIView):
    def get(self, request):
        phone = request.query_params.get("phone")
        payload = {"username": phone, "exp": datetime.datetime.utcnow() + datetime.timedelta(
            days=7)}
        jwt_token = jwt.encode(payload, settings.JWT_SECRET_KEY)
        print("jwt_token=", jwt_token)

        return Response(jwt_token, status=status.HTTP_200_OK)


class RightsInfo(APIView):
    """查询用户的权限信息 hb/rights/info"""

    def get(self, request):
        phone = request.query_params.get("phone")  # 手机号码
        if not re.match("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$", phone):
            return Response({"res": 1, "errmsg": "bad phone number format! 电话号码格式错误"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute("select rights from factory_users where phone = '%s';" % phone)
            rights = cursor.fetchone()
            rights = rights[0] if rights else []
            # print(result)
            cursor.execute("select rights from hb_roles where phone ='%s';" % phone)
            role = cursor.fetchone()
            role = role[0] if role else ""
            return Response({"rights": rights, "role": role}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class HbManager(APIView):
    """海滨医疗权限展示列表 hb/manager
    10代表app首页是否能看到 治疗仪管理 的权限
    首页海滨医疗入口权限 10 （部门）,app首页权限管理拉人但是没分配角色 10
    超级管理员 1 ——> admin
    普通管理员 common
    医生      doctor
    """
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        """管理员列表"""
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        sql_1 = """
        select 
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image
        from
          (
          select 
            *
          from 
            factory_users
          where 
            factory = 'hbyl' and rights = '{1}'
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone;
        """

        sql_2 = """
        select 
          t.rights,
          t.phone,
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image,
          COALESCE(t.time) as time,
          COALESCE(t.invitor, '') as invitor
        from
          (
          select 
            *
          from 
            hb_roles
          where 
            rights = 'common'
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone;
        """
        # print(sql_1, sql_2)

        try:
            cursor.execute(sql_1)
            result1 = cursor.fetchone()
            cursor.execute(sql_2)
            result2 = cursor.fetchall()
            # print(result1, result2)

            super_manager, manager = {}, []  # 超级管理员，普通管理员

            if result1:
                super_manager["name"] = result1[0]
                if isinstance(result1[1], memoryview):
                    temp = result1[1].tobytes().decode()
                    image_url = alioss.joint_image(temp)
                    super_manager["image"] = image_url
                elif isinstance(result1[1], str):
                    image_url = alioss.joint_image(result1[1])
                    super_manager["image"] = image_url
            if result2:
                for res in result2:
                    di = dict()
                    di["name"] = res[2]

                    if isinstance(res[3], memoryview):
                        temp = res[3].tobytes().decode()
                        image_url = alioss.joint_image(temp)
                        di["image"] = image_url
                    elif isinstance(res[3], str):
                        image_url = alioss.joint_image(res[3])
                        di["image"] = image_url

                    di["phone"] = res[1]
                    di["time"] = res[4] if res[4] else arrow.now().timestamp
                    invitor = res[5]
                    cursor.execute("select name from user_info where phone = '%s';" % invitor)
                    res = cursor.fetchone()
                    di["invitor"] = res[0] if res else ""
                    manager.append(di)

            return Response({"super_manager": super_manager, "manager": manager}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        """添加普通管理员"""
        manager_list = request.data.get("manager_list", [])  # list

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            for manager in manager_list:
                cursor.execute("select count(1) from hb_roles where phone = '%s';" % manager)
                phone_check = cursor.fetchone()[0]
                if phone_check >= 1:
                    return Response({"res": 1, "errmsg": "Phone already in current factory! 电话号码已是海滨医疗管理员或医生！"},
                                    status=status.HTTP_200_OK)

                cursor.execute("select count(1) from factory_users where factory = 'hbyl' and phone = '%s';" % manager)
                factory_check = cursor.fetchone()[0]
                if factory_check <= 0:
                    return Response({"res": 1, "errmsg": "Phone doesn't invited by HBYL! 此号码未被邀请加入海滨医疗！"},
                                    status=status.HTTP_200_OK)

                cursor.execute(
                    "insert into hb_roles (phone, rights, time, invitor) values ('%s', 'common', %d, '%s');" % (
                        manager, int(time.time()), phone))

            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request):
        """删除普通管理员"""
        manager = request.query_params.get("phone")
        if not manager:
            return Response({"res": 1, "errmsg": "缺少参数电话号码，无法删除！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        role = request.redis_cache["role"]
        # print(phone, factory_id, role)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select count(*) from factory_users where phone = '%s' and factory = '%s' and "
                           "'1' = ANY(rights);" % (manager, factory_id))
            result = cursor.fetchone()[0]
            if result >= 1:
                return Response({"res": 1, "errmsg": "该电话号码是超级管理员，不能删除权限！"}, status=status.HTTP_200_OK)

            cursor.execute("delete from hb_roles where phone = '%s';" % manager)
            connection.commit()

            # new_phone用户被删除，删除Redis的缓存
            redis_conn = get_redis_connection("default")
            pl = redis_conn.pipeline()
            pl.hdel(manager, "role", role)
            pl.hdel(manager, "factory_id", factory_id)
            pl.execute()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class UserList(APIView):
    """老板-刚拉进海滨医疗部门入口，权限为10的人，但是没有分配普通管理员/医生的角色 hb/user/list"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        sql = """
        select 
          t.rights,
          t.phone,
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image
        from
          (
          select 
            *,
            row_number() over (order by time desc) as rn
          from 
            factory_users
          where 
            factory = '%s' and '10' = any(rights) and phone not in 
            (
              select phone from hb_roles
            )
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone
        where rn > %d
        order by t.time desc
        limit %d;
        """ % (factory_id, offset, row)
        # print(sql)
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            data = []
            for res in result:
                di = dict()
                di["phone"] = res[1]
                di["name"] = res[2]
                if isinstance(res[3], memoryview):
                    temp = res[3].tobytes().decode()
                    image_url = alioss.joint_image(temp)
                    di["image"] = image_url
                elif isinstance(res[3], str):
                    image_url = alioss.joint_image(res[3])
                    di["image"] = image_url
                data.append(di)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class RightsOrg(APIView):
    """获取应用对应的权限列表 rights/orgs"""

    def get(self, request):
        rights_list = [{"name": "订单", "id": "3"}, {"name": "采购", "id": "5"}, {"name": "生产", "id": "7"},
                       {"name": "权限管理", "id": "8"}, {"name": "仓库", "id": "9"}, {"name": "治疗仪入口", "id": "10"},
                       {"name": "普通管理员", "id": "common"}, {"name": "医生", "id": "doctor"}]

        return Response({"list": rights_list}, status=status.HTTP_200_OK)


class PatientsMain(APIView):
    """老板-患者总览首页 /hb/patients/main"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        start_timestamp, end_timestamp = month_timestamp(datetime.datetime.now().year, datetime.datetime.now().month)
        data, patients_summary, recent_treatment, patients_analysis, age_stage, doctors_rank = {}, {}, [], {}, [], []
        try:
            cursor.execute("select count(1) from hb_patients t1 inner join hb_doctors t2 on t1.doctor_phone = "
                           "t2.doctor_phone where active = '0';")
            patients_count = cursor.fetchone()[0]
            cursor.execute("select count(1) from hb_hospitals where active = '0';")
            hospitals_count = cursor.fetchone()[0]
            cursor.execute("select count(1) from hb_devices;")
            equipments_count = cursor.fetchone()[0]
            cursor.execute("select count(distinct patient_id) from hb_treatment_logs where time >= %d and time < %d;"
                           % (start_timestamp, end_timestamp))
            this_month = cursor.fetchone()[0]

            if hospitals_count == 0:
                average_hospitals = patients_count
            else:
                average_hospitals = round(patients_count / hospitals_count)
            if equipments_count == 0:
                average_equipments = patients_count
            else:
                average_equipments = round(patients_count / equipments_count)

            patients_summary["total"] = patients_count
            patients_summary["average_hospitals"] = average_hospitals
            patients_summary["average_equipments"] = average_equipments
            patients_summary["this_month"] = this_month

            # 首页最近5天的数据统计
            date_list = [arrow.now().shift(days=-i).format("YYYY-MM-DD") for i in range(4, -1, -1)]
            cursor.execute("select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as day, count(1) from hb_treatment_logs "
                           "group by day order by day desc;")
            result = dict(cursor.fetchall())
            # print("result=", result), print("date_list=", date_list)
            # 改成键值对格式
            for date in date_list:
                if date in result:
                    recent_treatment.append({"date": date, "count": result[date]})
                else:
                    recent_treatment.append({"date": date, "count": 0})

            cursor.execute("select count(gender), gender from hb_patients group by gender;")
            result1 = cursor.fetchall()
            # 性别 0：男，1：女
            male, female = 0, 0
            for res in result1:
                if res[1] == "0":
                    male += res[0]
                elif res[1] == "1":
                    female += res[0]

            sql2 = """
            select 
              coalesce(sum(CASE WHEN date_part('year', current_date) - date_part('year', birthday) < 18 
                THEN 1 ELSE 0 END), 0) as count1,
              coalesce(sum(CASE WHEN date_part('year', current_date) - date_part('year', birthday) 
                between 19 and 35 THEN 1 ELSE 0 END), 0) as count2,
              coalesce(sum(CASE WHEN date_part('year', current_date) - date_part('year', birthday) 
                between 36 and 59 THEN 1 ELSE 0 END), 0) as count3,
              coalesce(sum(CASE WHEN date_part('year', current_date) - date_part('year', birthday) > 60 
              THEN 1 ELSE 0 END), 0) as count4
            from 
              hb_patients;
            """
            cursor.execute(sql2)
            result2 = cursor.fetchone()
            count1, count2, count3, count4 = result2[0], result2[1], result2[2], result2[3]
            if patients_count == 0:
                percent1, percent2, percent3, percent4 = 0, 0, 0, 0
            else:
                percent1 = "%.2f" % ((count1 / patients_count) * 100) + "%"
                percent2 = "%.2f" % ((count2 / patients_count) * 100) + "%"
                percent3 = "%.2f" % ((count3 / patients_count) * 100) + "%"
                percent4 = "%.2f" % ((count4 / patients_count) * 100) + "%"

            age_stage.append({"stage": "1", "count": count1, "percent": percent1})
            age_stage.append({"stage": "2", "count": count2, "percent": percent2})
            age_stage.append({"stage": "3", "count": count3, "percent": percent3})
            age_stage.append({"stage": "4", "count": count4, "percent": percent4})

            patients_analysis["male"] = male
            patients_analysis["female"] = female
            patients_analysis["age_stage"] = age_stage

            sql3 = """
            select 
              t1.count, coalesce(t2.name, ''), t2.image 
            from 
              (
                select 
                  count(1) as count, doctor_id 
                from 
                  hb_treatment_logs 
                where 
                  doctor_id in 
                  (
                    select 
                      doctor_phone
                    from 
                      hb_doctors
                    where 
                      active = '0'
                  )
                group by doctor_id
              ) t1 
            left join 
              user_info t2 
            on 
              t1.doctor_id = t2.phone
            order by t1.count desc 
            limit 3;
            """
            cursor.execute(sql3)
            result3 = cursor.fetchall()
            for i, res in enumerate(result3):
                di = dict()
                di["rn"] = i + 1
                di["count"] = res[0]
                di["name"] = res[1]
                if isinstance(res[2], memoryview):
                    image = res[2].tobytes().decode()
                    image_url = alioss.joint_image(image)
                    di["image"] = image_url
                elif isinstance(res[2], str):
                    image_url = alioss.joint_image(res[2])
                    di["image"] = image_url
                doctors_rank.append(di)

            data["patients_summary"] = patients_summary
            data["recent_treatment"] = recent_treatment
            data["patients_total"] = patients_count
            cursor.execute("select count(1) from hb_treatment_logs;")
            data["records_total"] = cursor.fetchone()[0]
            data["patients_analysis"] = patients_analysis
            data["doctors_rank"] = doctors_rank

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PatientsStatistics(APIView):
    """设备使用统计 hb/patients/statistics/(\w+)
    有5种情况：
    1.老板-设备-设备使用统计：所有设备，equipment_id="", hospital_id="", usage="devices"
    2.老板-设备-设备使用统计：单台设备，只传equipment_id, hospital_id="", usage="devices"
    3.老板-医院-设备使用统计：某个医院下的单台设备，要传equipment_id和hospital_id, usage="devices"
    4.老板-医院-设备使用统计：某个医院下的所有设备，equipment_id="", 要传hospital_id, usage="devices"
    5.患者-治疗患者统计：equipment_id="", hospital_id="", usage="patients"
    """
    permission_classes = [CommonAdminPermission]

    def get(self, request, choice):
        # choice ——> day:天, week:周, month:月
        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        equipment_id = request.query_params.get("equipment_id")  # 设备id
        hospital_id = request.query_params.get("hospital_id")  # 医院id
        usage = request.query_params.get("usage")  # 复用：patients/devices

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        if usage == "patients":
            condition1 = " count(distinct patient_id) "
        elif usage == "devices":
            condition1 = " count(1) "
        else:
            return Response({"res": 1, "errmsg": "复用代号错误！"}, status=status.HTTP_200_OK)

        if not equipment_id and not hospital_id:
            # 老板-设备-设备使用统计：所有设备/患者-治疗患者统计：所有设备
            condition2 = ""
        elif equipment_id and not hospital_id:
            # 老板-设备-设备使用统计：单台设备
            condition2 = " where equipment_id = '%s' " % equipment_id
        elif equipment_id and hospital_id:
            # 老板-医院-设备使用统计：某个医院下的单台设备
            condition2 = " where equipment_id = '%s' and hospital_id = '%s' " % (equipment_id, hospital_id)
        elif not equipment_id and hospital_id:
            # 老板-医院-设备使用统计：某个医院下的所有设备
            condition2 = " where hospital_id = '%s' " % hospital_id
        else:
            return Response({"res": 1, "errmsg": "查询字符串中参数传递错误！"}, status=status.HTTP_200_OK)

        # 使用记录-患者信息
        sql_records = """
        select
          t1.*,
          coalesce(t2.patient_name, '') as name,
          coalesce(t2.gender, '') as gender,
          t2.birthday
        from
          (
            select 
              patient_id,
              body_parts,
              gear_position,
              pulse_counts,
              time,
              row_number() over (order by time desc) as rn
            from 
              hb_treatment_logs """ + condition2 + """
          ) t1
        left join hb_patients t2 on 
          t1.patient_id = t2.patient_phone
        where rn > %d 
        order by t1.time desc
        limit %d;
        """ % (offset, row)
        # print(sql_records)

        data, summary, records, date_list, result = {}, [], [], [], []

        try:
            if choice == "day":
                # 过去 按天 日期列表
                sql_day = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as day, " + condition1 + " from hb_treatment_logs " + condition2 + " group by day order by day desc;"
                # print(sql_day)
                cursor.execute(sql_day)
                result = cursor.fetchall()

                if result:
                    start = result[-1][0]
                    range_days = (arrow.now() - arrow.get(start)).days
                    # date_list = [arrow.now().shift(days=-i).format("YYYY-MM-DD") for i in range(range_days, -1, -1)]
                    date_list = [arrow.now().shift(days=-i).format("YYYY-MM-DD") for i in range(4, -1, -1)]
            elif choice == "week":
                # 过去按 星期 日期列表
                sql_day = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as day, " + condition1 + " from hb_treatment_logs " + condition2 + " group by day order by day desc;"
                cursor.execute(sql_day)
                result_day = cursor.fetchall()

                if result_day:
                    start = result_day[-1][0]
                    range_days = (arrow.now() - arrow.get(start)).days
                else:
                    range_days = 0

                # ISO Year和ISO Week的格式，ISO Week有时候只有52周，PostgreSQL的格式化'YYYY-WW'会有53周
                sql_week = "select to_char(TO_TIMESTAMP(time), 'iyyy-IW') as week, " + condition1 + " from hb_treatment_logs " + condition2 + " group by week order by week desc;"
                cursor.execute(sql_week)
                result = cursor.fetchall()

                if result:
                    # range_weeks = range_days // 7 + 1
                    range_weeks = 4
                    date_list = [
                        str(arrow.now().shift(weeks=-i).isocalendar()[0]) + "-" + str(
                            arrow.now().shift(weeks=-i).isocalendar()[1]).zfill(2)
                        for i in
                        range(range_weeks, -1, -1)]
            elif choice == "month":
                # 过去 按月 日期列表
                sql_month = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM') as month, " + condition1 + " from hb_treatment_logs " + condition2 + " group by month order by month desc;"
                cursor.execute(sql_month)
                result = cursor.fetchall()

                if result:
                    start = result[-1][0]
                    range_months = abs((arrow.now().date().year - arrow.get(
                        start).date().year)) * 12 + arrow.now().date().month - arrow.get(start).date().month
                    # date_list = [arrow.now().shift(months=-i).format("YYYY-MM") for i in range(range_months, -1, -1)]
                    date_list = [arrow.now().shift(months=-i).format("YYYY-MM") for i in range(4, -1, -1)]
            else:
                return Response({"res": 1, "errmsg": "时间类型参数错误！"})

            temp = dict(result)
            # print("temp=", temp), print("date_list=", date_list)
            for date in date_list:
                if choice == "week":
                    # 从ISO week周数获取对应的日期列表, datetime.date类型
                    transform_list = isoweek.Week(int(date.split("-")[0]), int(date.split("-")[-1])).days()
                    # 日期列表，字符串
                    days_list = [day.strftime("%Y-%m-%d") for day in transform_list]
                    monday = days_list[0].replace("-", ".").split('.')[1:]
                    sunday = days_list[-1].replace("-", ".").split('.')[1:]
                    if date in temp:
                        summary.append(
                            {"iso_weeks": date.replace("-", "."), "count": temp[date],
                             "date": '.'.join(monday) + "-" + '.'.join(sunday)})
                    else:
                        summary.append(
                            {"iso_weeks": date.replace("-", "."), "count": 0,
                             "date": '.'.join(monday) + "-" + '.'.join(sunday)})
                else:
                    if date in temp:
                        summary.append({"date": date.replace("-", "."), "count": temp[date]})
                    else:
                        summary.append({"date": date.replace("-", "."), "count": 0})

            cursor.execute(sql_records)
            temp2 = cursor.fetchall()
            for t in temp2:
                di, treatments = dict(), list()
                body_parts, gear_position, pulse_counts = t[1], t[2], t[3]
                treat = list(zip(body_parts, gear_position, pulse_counts))
                for tr in treat:
                    dt = dict()
                    dt["treatment_part"] = str(tr[0])
                    dt["treatment_gear"] = str(tr[1])
                    dt["treatment_pulse"] = str(tr[2])
                    treatments.append(dt)
                di["treatments"] = treatments
                di["patient_id"] = t[0]
                di["time"] = t[4] or 0
                di["name"] = t[6]
                di["gender"] = t[7]
                if t[8]:
                    di["age"] = str(datetime.datetime.now().year - int(t[8].strftime('%Y')))
                else:
                    di["age"] = ""

                records.append(di)

            data["summary"], data["records"] = summary, records

            return Response(data, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PatientsList(APIView):
    """医院-某个医院的全部患者/医院-医生管理-某个医生的患者/全部患者 hb/patients/list"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        type_ = request.query_params.get("type")  # hospital:按医院, doctor：按医生，null：全部
        id_ = request.query_params.get("id")  # 医院id/医生id/null
        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        data = []

        if type_ == "hospital" and id_:  # 某个医院的患者
            condition1 = ""
            condition2 = " and hospital_id = '%s' " % id_
        elif type_ == "doctor" and id_:  # 某个医生的患者
            condition1 = " and doctor_phone = '%s' " % id_
            condition2 = ""
        elif not type_ and not id_:  # 所有的患者
            condition1 = ""
            condition2 = ""
        else:
            return Response({"res": 1, "errmsg": "传递参数错误！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.patient_phone,
          coalesce(t1.patient_name, '') as name,
          t1.gender,
          t1.birthday,
          coalesce(t3.hospital_name, '') as hospital_name
        from
          (
            select
              *,
              row_number() over (order by time desc) as rn
            from
              hb_patients
          ) t1
        inner join 
          (
            select 
              *
            from 
              hb_doctors
            where active = '0' """ + condition1 + """
          ) t2 on
          t1.doctor_phone = t2.doctor_phone
        inner join  
          (
            select 
              *
            from 
              hb_hospitals
            where 
              active = '0' """ + condition2 + """
          ) t3 on 
          t2.hospital_id = t3.hospital_id
        where rn > %d 
        order by t1.time desc
        limit %d;
        """ % (offset, row)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            for res in result:
                di, treatment_part = dict(), list()
                cursor.execute("select body_parts from hb_treatment_logs where patient_id = '%s';" % res[0])
                body_parts_list = cursor.fetchall()

                for body in body_parts_list:
                    treatment_part += body[0]
                di["treatment_part"] = list(set(treatment_part))
                di["id"] = res[0]
                di["name"] = res[1]
                di["gender"] = res[2]
                if res[3]:
                    di["age"] = str(datetime.datetime.now().year - int(res[3].strftime('%Y')))
                else:
                    di["age"] = ""
                di["hospital"] = res[4]
                data.append(di)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PatientsTreatmentRecords(APIView):
    """患者-某种治疗记录/医院-医生详情-治疗记录 hb/treatment/records"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        type_ = request.query_params.get("type")  # hospital: 按医院, doctor:，null：全部
        id_ = request.query_params.get("id")  # 医院id/医生id/null
        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        if type_ == "hospital" and id_:  # 某个医院的治疗记录
            condition = """
            where hospital_id = '%s' and hospital_id in 
                (select hospital_id from hb_hospitals where active = '0') """ % id_
        elif type_ == "doctor" and id_:  # 某个医生的治疗记录
            condition = """ 
            where doctor_id = '%s' and doctor_id in 
                (select doctor_phone from hb_doctors where active = '0')""" % id_
        elif not type_ and not id_:  # 所有的治疗记录
            condition = ""
        else:
            return Response({"res": 1, "errmsg": "传递参数错误！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.patient_id,
          t1.body_parts,
          t1.gear_position,
          t1.pulse_counts,
          coalesce(t1.remark, '') as remark,
          coalesce(t1.time, 0) as time,
          coalesce(t2.patient_name, '') as name,
          coalesce(t2.gender, '') as gender,
          t2.birthday
        from 
          (
            select
              *,
              row_number() over (order by time desc) as rn
            from
              hb_treatment_logs """ + condition + """
          ) t1
        left join hb_patients t2 on
          t1.patient_id = t2.patient_phone
        where rn > %d 
        order by t1.time desc
        limit %d;
        """ % (offset, row)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            data = []
            for res in result:
                di, treatments = dict(), list()
                body_parts, gear_position, pulse_counts = res[1], res[2], res[3]
                treat = list(zip(body_parts, gear_position, pulse_counts))
                for tr in treat:
                    dt = dict()
                    dt["treatment_part"] = str(tr[0])
                    dt["treatment_gear"] = str(tr[1])
                    dt["treatment_pulse"] = str(tr[2])
                    treatments.append(dt)
                di["treatments"] = treatments
                di["id"] = res[0]
                di["remark"] = res[4]
                di["time"] = res[5]
                di["name"] = res[6]
                di["gender"] = res[7]
                if res[8]:
                    di["age"] = str(datetime.datetime.now().year - int(res[8].strftime('%Y')))
                else:
                    di["age"] = ""

                data.append(di)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PatientDetail(APIView):
    """患者-某个患者详情 hb/patients/detail/{id}"""
    permission_classes = [AllPermission]

    def get(self, request, id):
        if not id:
            return Response({"res": 1, "errmsg": "缺少参数用户id！"}, status=status.HTTP_200_OK)

        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        sql_1 = """
        select 
          t1.id,
          t1.patient_phone,
          t1.gender,
          t1.birthday,
          coalesce(t1.patient_name, '') as patient_name,
          coalesce(t1.region, '') as region,
          coalesce(t1.address, '') as address,
          coalesce(t1.medical_history, '') as medical_history,
          coalesce(t1.before_healthcare, '') as before_healthcare,
          coalesce(t1.after_healthcare, '') as after_healthcare,
          coalesce(t1.remark, '') as remark,
          coalesce(t3.hospital_name, '') as hospital_name
        from
          (
            select 
              *
            from 
              hb_patients
            where 
              patient_phone = '%s'
          ) t1 
        inner join 
          (
            select 
              *
            from
              hb_doctors 
            where active = '0'
          ) t2 on
          t1.doctor_phone = t2.doctor_phone
        inner join 
          (
            select 
              *
            from
              hb_hospitals
            where active = '0'
          ) t3 on 
          t2.hospital_id = t3.hospital_id;
        """ % id

        sql_2 = """
        select
          t2.id as equipment_id,
          coalesce(t2.name, '') as equipment_name,
          coalesce(t2.type, '') as equipment_category
        from
          (
            select
              row_number() over (order by time desc) as rn,
              time,
              equipment_id
            from
              hb_treatment_logs
            where 
              patient_id = '%s'
            limit 1
          ) t1
        left join hb_devices t2 on
          t1.equipment_id = t2.id;
        """ % id

        sql_3 = """
        select
          t1.body_parts,
          t1.gear_position,
          t1.pulse_counts,
          coalesce(t1.remark, '') as remark,
          coalesce(to_char(to_timestamp(t1.time), 'MM-DD'), '') as time,
          coalesce(t3.name, '') as name,
          coalesce(t3.image, '') as image
        from
          (
            select
              *,
              row_number() over (order by time desc) as rn
            from
              hb_treatment_logs
            where 
              patient_id = '%s' and doctor_id in 
              (select doctor_id from hb_doctors where active = '0')
          ) t1
        left join hb_patients t2 on
          t1.patient_id = t2.patient_phone
        left join user_info t3
          on t1.doctor_id = t3.phone
        where rn > %d 
        order by t1.time desc
        limit %d;
        """ % (id, offset, row)

        try:
            cursor.execute(sql_1)
            result1 = cursor.fetchall()
            cursor.execute(sql_2)
            result2 = cursor.fetchall()
            cursor.execute(sql_3)
            result3 = cursor.fetchall()
            # print("result1=", result1), print("result2=", result2), print("result3=", result3)

            patients_summary, device, records, record_list = {}, {}, {}, []
            if result1:
                for res in result1:
                    patients_summary["id"] = str(res[0]).zfill(6)
                    patients_summary["phone"] = res[1]
                    patients_summary["gender"] = res[2]
                    if res[3]:
                        patients_summary["age"] = str(datetime.datetime.now().year - int(res[3].strftime('%Y')))
                        patients_summary["birthday"] = res[3]
                    else:
                        patients_summary["age"] = ""
                        patients_summary["birthday"] = ""
                    patients_summary["name"] = res[4]
                    patients_summary["region"] = res[5]
                    patients_summary["address"] = res[6]
                    patients_summary["medical_history"] = res[7]
                    patients_summary["before_healthcare"] = res[8]
                    patients_summary["after_healthcare"] = res[9]
                    patients_summary["remark"] = res[10]
                    patients_summary["hospital"] = res[11]
            if result2:
                for res in result2:
                    device["id"] = res[0]
                    device["name"] = res[1]
                    device["categroy"] = res[2]
            if result3:
                for res in result3:
                    di, detail = dict(), list()
                    temp = list(zip(res[0], res[1], res[2]))
                    for t in temp:
                        dt = dict()
                        dt["treatment_part"] = t[0]
                        dt["treatment_gear"] = t[1]
                        dt["treatment_pulse"] = t[2]
                        detail.append(dt)
                    di["detail"] = detail
                    di["remark"] = res[3]
                    # di["date"] = res[4]
                    di['month'], di["day"] = res[4].split('-')
                    di["doctor"] = res[5]
                    if isinstance(res[6], memoryview):
                        image = res[6].tobytes().decode()
                        image_url = alioss.joint_image(image)
                        di["avatar"] = image_url
                    elif isinstance(res[6], str):
                        image_url = alioss.joint_image(res[6])
                        di["avatar"] = image_url
                    record_list.append(di)

            records["count"] = len(result3)
            records["record_list"] = record_list

            return Response({"patients_summary": patients_summary, "device": device, "records": records},
                            status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, id):
        """修改患者详情"""
        if not id:
            return Response({"res": 1, "errmsg": "缺少参数用户id！"}, status=status.HTTP_200_OK)

        patient_name = request.data.get("patient_name")
        gender = request.data.get("gender")  # 性别 0：男，1：女
        birthday = request.data.get("birthday")
        new_phone = request.data.get("phone")
        region = request.data.get("region", "")
        address = request.data.get("address", "")
        medical_history = request.data.get("medical_history", "")
        before_healthcare = request.data.get("before_healthcare", "")
        after_healthcare = request.data.get("after_healthcare", "")
        remark = request.data.get("remark", "")

        if not all([patient_name, gender, birthday, new_phone]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from hb_patients where patient_phone = '%s';" % id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在，无法修改！"}, status=status.HTTP_200_OK)

        cursor.execute("select doctor_phone from hb_patients where patient_phone = '%s';" % id)
        doctor_phone = cursor.fetchone()
        doctor_phone = doctor_phone[0] if doctor_phone else ""
        # print("doctor_phone=", doctor_phone)

        # 注意：patient_phone为主键，不允许重复。要先删掉此手机号，然后检查新手机号是否重复，最后一起commit提交。
        """
        1.手机号未做修改，还是原来手机号。不能做重复性校验，会把自身的数量算进去为1。但是输入重复手机号时会报错。所以直接用delete删除然后insert比较好
        2.手机号修改了，变为新手机号。
        """
        cursor.execute("delete from hb_patients where patient_phone = '%s';" % id)

        cursor.execute("select count(1) from hb_patients where patient_phone = '%s';" % new_phone)
        new_phone_check = cursor.fetchone()[0]
        if new_phone_check >= 1:
            return Response({"res": 1, "errmsg": "This phone number is already exist! 此电话号码已经存在！"},
                            status=status.HTTP_200_OK)

        sql_1 = "insert into hb_patients (patient_phone, doctor_phone, patient_name, gender, birthday, region, " \
                "address, medical_history, before_healthcare, after_healthcare, remark, time) VALUES " \
                "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d)" % \
                (new_phone, doctor_phone, patient_name, gender, birthday, region, address, medical_history,
                 before_healthcare, after_healthcare, remark, int(time.time()))
        sql_2 = "update hb_treatment_logs set patient_id = '%s' where patient_id = '%s';" % (new_phone, id)

        try:
            cursor.execute(sql_1)
            cursor.execute(sql_2)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        """删除患者"""
        if not id:
            return Response({"res": 1, "errmsg": "缺少参数用户id！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from hb_patients where patient_phone = '%s';" % id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在，无法删除！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("delete from hb_patients where patient_phone = '%s';" % id)
            cursor.execute("delete from hb_treatment_logs where patient_id = '%s';" % id)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


# 医生-------------------------------------------------------------------------------------------------------------------
class PatientNew(APIView):
    """添加患者 hb/doctor/patient/new"""
    permission_classes = [DoctorPermission]

    def post(self, request):
        patient_name = request.data.get("patient_name")
        gender = request.data.get("gender")  # 性别 0：男，1：女
        birthday = request.data.get("birthday")
        patient_phone = request.data.get("phone")
        region = request.data.get("region", "")
        address = request.data.get("address", "")
        medical_history = request.data.get("medical_history", "")
        before_healthcare = request.data.get("before_healthcare", "")
        after_healthcare = request.data.get("after_healthcare", "")
        remark = request.data.get("remark", "")

        if not all([patient_name, gender, birthday, patient_phone]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)
        if gender not in ["0", "1"]:
            return Response({"res": 1, "errmsg": "性别代号错误！"}, status=status.HTTP_200_OK)
        if not re.match("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$", patient_phone):
            return Response({"res": 1, "errmsg": "电话号码格式错误"}, status=status.HTTP_200_OK)

        doctor_phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from hb_patients where patient_phone = '%s';" % patient_phone)
        phone_check = cursor.fetchone()[0]
        if phone_check >= 1:
            return Response({"res": 1, "errmsg": "电话号码已存在！"}, status=status.HTTP_200_OK)

        sql = "insert into hb_patients (patient_phone, doctor_phone, patient_name, gender, birthday, region, " \
              "address, medical_history, before_healthcare, after_healthcare, remark, time) values " \
              "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d);" \
              % (patient_phone, doctor_phone, patient_name, gender, birthday, region, address, medical_history,
                 before_healthcare, after_healthcare, remark, int(time.time()))

        try:
            cursor.execute(sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class DoctorMain(APIView):
    """医生-首页 hb/doctor/main"""
    permission_classes = [DoctorPermission]

    def get(self, request):
        doctor_phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        sql_1 = """
        select
          t1.id,
          t1.hospital_id,
          coalesce(t2.hospital_name, '') as hospital_name,
          coalesce(t3.name, '') as doctor_name,
          coalesce(t3.image, '') as avatar
        from 
        (
          select
            *
          from
            hb_doctors
          where active = '0' and doctor_phone = '%s'
        ) t1
        left join hb_hospitals t2 on
          t1.hospital_id = t2.hospital_id
        left join user_info t3 on
          t1.doctor_phone = t3.phone;
        """ % doctor_phone

        # 过去4天日期列表
        date_list = [arrow.now().shift(days=-i).format("YYYY-MM-DD") for i in range(3, -1, -1)]
        sql_2 = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as day, count(1) from hb_treatment_logs" \
                " where hospital_id = '%s' group by day order by day desc;"

        sql_3 = """
        select
          id,
          name,
          type,
          aval_times
        from
          hb_devices t1
        left join hb_equipments t2 on 
          t1.id = t2.equipment_id
        where t2.hospital_id = '%s';
        """

        data, recent_counts, devices = {}, [], []

        try:
            cursor.execute(sql_1)
            result1 = cursor.fetchone()

            if result1:
                data["id"] = str(result1[0]).zfill(6)
                hospital_id = result1[1]
                data["hospital_name"] = result1[2]
                data["doctor_name"] = result1[3]
                if isinstance(result1[4], memoryview):
                    temp = result1[4].tobytes().decode()
                    image_url = alioss.joint_image(temp)
                    data["avatar"] = image_url
                elif isinstance(result1[4], str):
                    image_url = alioss.joint_image(result1[4])
                    data["avatar"] = image_url
            else:
                hospital_id = ""
                data["id"], data["hospital_name"], data["doctor_name"], data["avatar"] = "", "", "", alioss.joint_image(
                    "")

            cursor.execute(sql_2 % hospital_id)
            result2 = dict(cursor.fetchall())
            # print("result2=", result2), print("date_list=", date_list)
            for date in date_list:
                if date in result2:
                    recent_counts.append({"date": date, "count": result2[date]})
                else:
                    recent_counts.append({"date": date, "count": 0})

            cursor.execute(sql_3 % hospital_id)
            result3 = cursor.fetchall()
            for res in result3:
                di = dict()
                di["equipment_id"] = res[0]
                di["equipment_name"] = res[1]
                di["equipment_category"] = res[2]
                di["available_counts"] = res[3]
                devices.append(di)

            data["recent_counts"], data["devices"] = recent_counts, devices
            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class DoctorHbManager(APIView):
    """医生-普通管理员列表 hb/doctor/manager"""
    permission_classes = [DoctorPermission]

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        alioss = AliOss()

        sql = """
        select 
          t.phone,
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image
        from
          (
          select 
            *
          from 
            hb_roles
          where 
            rights = 'common'
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone;
        """
        # print(sql)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            manager = []  # 普通管理员
            for res in result:
                di = dict()
                di["phone"] = res[0]
                di["name"] = res[1]

                if isinstance(res[2], memoryview):
                    temp = res[2].tobytes().decode()
                    image_url = alioss.joint_image(temp)
                    di["image"] = image_url
                elif isinstance(res[2], str):
                    image_url = alioss.joint_image(res[2])
                    di["image"] = image_url
                manager.append(di)

            return Response({"manager": manager}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class DoctorTreatmentRecordNew(APIView):
    """医生-添加治疗记录 hb/doctor/records/new"""
    permission_classes = [DoctorPermission]

    def post(self, request):
        patient_phone = request.data.get("id")  # 患者手机号
        equipment_id = request.data.get("equipment_id")  # 设备id
        remark = request.data.get("remark", "")  # 备注
        treatments = request.data.get("treatments", [])  # list

        doctor_phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(doctor_phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select hospital_id from hb_doctors where doctor_phone = '%s' and active = '0';" % doctor_phone)
        result1 = cursor.fetchone()
        if result1:
            hospital_id = result1[0]
        else:
            return Response({"res": 1, "errmsg": "此医生不存在！"}, status=status.HTTP_200_OK)

        sql = """
        insert into
          hb_treatment_logs
          (patient_id, equipment_id, doctor_id, hospital_id, body_parts, gear_position, pulse_counts, remark, time)
        values 
          ('%s', '%s', '%s', '%s', '{%s}', '{%s}', '{%s}', '%s', %d);
        """
        try:
            part_list, gear_list, pulse_list = [], [], []
            for treat in treatments:
                if not treat["treatment_gear"].isdigit() or not treat["treatment_pulse"].isdigit():
                    return Response({"res": 1, "errmsg": "治疗档位或脉冲次数请填写数字！"}, status=status.HTTP_200_OK)
                part_list.append(treat["treatment_part"])
                gear_list.append(treat["treatment_gear"])
                pulse_list.append(treat["treatment_pulse"])
            cursor.execute(sql % (patient_phone, equipment_id, doctor_phone, hospital_id, ','.join(part_list),
                                  ','.join(gear_list), ','.join(pulse_list), remark, int(time.time())))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class DoctorPatientsList(APIView):
    """医生-某个医生的患者，不显示其它医生的患者 hb/doctor/patients/list"""
    permission_classes = [DoctorPermission]

    def get(self, request):
        page = int(request.query_params.get("page", 1))
        row = int(request.query_params.get("row", ROW))
        offset = (page - 1) * row

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        data = []

        id_ = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        sql = """
        select
          t1.patient_phone,
          coalesce(t1.patient_name, '') as name,
          t1.gender,
          t1.birthday,
          coalesce(t3.hospital_name, '') as hospital_name,
          t3.hospital_id
        from
          (
            select
              *,
              row_number() over (order by time desc) as rn
            from
              hb_patients
            where doctor_phone = '%s'
          ) t1
        inner join 
          (
            select 
              *
            from 
              hb_doctors
            where active = '0'
          ) t2 on
          t1.doctor_phone = t2.doctor_phone
        inner join 
          (
            select 
              *
            from 
              hb_hospitals
            where 
              active = '0'
          ) t3 on 
          t2.hospital_id = t3.hospital_id
        where rn > %d 
        order by t1.time desc
        limit %d;
        """ % (id_, offset, row)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            for res in result:
                di, treatment_part = dict(), list()
                cursor.execute("select body_parts from hb_treatment_logs where patient_id = '%s';" % res[0])
                body_parts_list = cursor.fetchall()

                for body in body_parts_list:
                    treatment_part += body[0]
                di["treatment_part"] = list(set(treatment_part))
                di["id"] = res[0]
                di["name"] = res[1]
                di["gender"] = res[2]
                if res[3]:
                    di["age"] = str(datetime.datetime.now().year - int(res[3].strftime('%Y')))
                else:
                    di["age"] = ""
                di["hospital"] = res[4]
                di["hospital_id"] = res[5]
                data.append(di)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class DoctorTreatmentRecords(APIView):
    """医生-某个患者上次的治疗记录 hb/doctor/treatment/records"""
    permission_classes = [DoctorPermission]

    def get(self, request):
        id_ = request.query_params.get("id")  # 某个患者的手机号

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
          t1.patient_id,
          t1.body_parts,
          t1.gear_position,
          t1.pulse_counts,
          coalesce(t1.remark, '') as remark,
          coalesce(t1.time, 0) as time,
          coalesce(t2.patient_name, '') as name,
          t2.gender,
          t2.birthday
        from 
          (
            select
              *,
              row_number() over (order by time desc) as rn
            from
              hb_treatment_logs
            where patient_id = '%s'
          ) t1
        left join hb_patients t2 on
          t1.patient_id = t2.patient_phone
        order by t1.time desc
        limit 1;
        """ % id_

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            data = []
            for res in result:
                di, treatments = dict(), list()
                body_parts, gear_position, pulse_counts = res[1], res[2], res[3]
                treat = list(zip(body_parts, gear_position, pulse_counts))
                for tr in treat:
                    dt = dict()
                    dt["treatment_part"] = str(tr[0])
                    dt["treatment_gear"] = str(tr[1])
                    dt["treatment_pulse"] = str(tr[2])
                    treatments.append(dt)
                di["treatment_detail"] = treatments
                di["id"] = res[0]
                di["remark"] = res[4]
                di["time"] = res[5]
                di["name"] = res[6]
                di["gender"] = res[7]
                if res[8]:
                    di["age"] = str(datetime.datetime.now().year - int(res[8].strftime('%Y')))
                else:
                    di["age"] = ""

                data.append(di)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


# 设备充值----------------------------------------------------------------------------------------------------------------
class EquipmentRecharge(APIView):
    """老板-使用次数充值 hb/recharge/new"""
    permission_classes = [CommonAdminPermission]

    def post(self, request):
        device_id = request.data.get("device_id")  # 设备id
        recharge_times = int(request.data.get("recharge_times"))  # 充值次数

        if not all([device_id, recharge_times]):
            return Response({"res": 1, "errmsg": "缺少参数"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        try:
            cursor.execute("select count(1) from hb_devices where id = '%s';" % device_id)
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "此id不存在，无法充值！"}, status=status.HTTP_200_OK)

            uuid = generate_uuid()
            cursor.execute("insert into hb_recharge_logs (id, equipment_id, recharge_counts, user_phone, time) VALUES "
                           "('%s', '%s', %d, '%s', %d);" % (uuid, device_id, recharge_times, phone, int(time.time())))
            connection.commit()

            # 发送消息通知
            message = {'resource': 'PyRecharge', 'type': 'POST',
                       'params': {'uuid': uuid, 'phone': phone, 'equipment_id': device_id,
                                  'recharge_counts': recharge_times}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
