import logging
import arrow
import calendar

from django.db import connection as conn
from django_redis import get_redis_connection
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import *
from permissions import CommonAdminPermission, AllPermission

logger = logging.getLogger('django')

# V1.0.0  jichengjian---------------------------------------------------------------------------------------------------
# todo 如果异常，返回数值为0的数据，还是报错？


class HbHospital(APIView):
    """get 获取医院信息  hb/hospital"""
    """post 新增医院  hb/hospital"""
    """put 修改医院信息  hb/hospital"""
    """delete 删除医院  hb/hospital"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        hospital_id = request.query_params.get("hospital_id")

        sql = "select hospital_id, hospital_name, contacts, phone, region, address from hb_hospitals where " \
              "hospital_id = '{}';".format(hospital_id)
        target = ['hospital_id', 'hospital_name', 'contacts', 'phone', 'region', 'address']

        try:
            cur.execute(sql)
            result = dict(zip(target, cur.fetchone()))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)

    def post(self, request):
        cur = conn.cursor()

        name = request.data.get("hospital_name")
        contact = request.data.get("contacts", "")
        phone = request.data.get("phone")
        region = request.data.get("region", "")
        address = request.data.get("address", "")
        uuid = generate_uuid()
        Time = int(time.time())
        # 此处相关界面应该添加是否恢复历史数据的选项
        sql_0 = "select count(1) from hb_hospitals where hospital_name = '{}' and active = '0';".format(name)
        sql_1 = "select count(1) from hb_hospitals where phone = '{}' and active = '0';".format(phone)
        sql_2 = "insert into hb_hospitals(hospital_id, hospital_name, contacts, phone, region, address, time) values " \
                "('{}', '{}', '{}', '{}', '{}', '{}', {});".format(uuid, name, contact, phone, region, address, Time)
        try:
            # 校验医院名称是否重复
            cur.execute(sql_0)
            name_check = cur.fetchone()[0]
            # 校验联系人手机号是否重复
            cur.execute(sql_1)
            phone_check = cur.fetchone()[0]
            if name_check != 0:
                return Response({"res": 1, "errmsg": "该医院名称已存在！"}, status=status.HTTP_200_OK)
            elif phone_check != 0:
                return Response({"res": 1, "errmsg": "该手机号已存在！"}, status=status.HTTP_200_OK)
            else:
                cur.execute(sql_2)
                conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)

    def put(self, request):
        cur = conn.cursor()

        hospital_id = request.data.get("hospital_id")
        name = request.data.get("hospital_name")
        contact = request.data.get("contacts", "")
        phone = request.data.get("phone")
        region = request.data.get("region", "")
        address = request.data.get("address", "")

        sql_0 = "select count(1) from hb_hospitals where hospital_name = '{}' and active = '0' and hospital_id != " \
                "'{}';".format(name, hospital_id)
        sql_1 = "select count(1) from hb_hospitals where phone = '{}' and active = '0' and hospital_id != " \
                "'{}';".format(phone, hospital_id)
        sql_2 = "update hb_hospitals set hospital_name = '{}', contacts = '{}', phone = '{}', region = '{}', address " \
                "= '{}' where hospital_id = '{}';".format(name, contact, phone, region, address, hospital_id)

        try:
            # 校验医院名称是否重复
            cur.execute(sql_0)
            name_check = cur.fetchone()[0]
            # 校验联系人手机号是否重复
            cur.execute(sql_1)
            phone_check = cur.fetchone()[0]
            if name_check != 0:
                return Response({"res": 1, "errmsg": "该医院名称已存在！"}, status=status.HTTP_200_OK)
            elif phone_check != 0:
                return Response({"res": 1, "errmsg": "该手机号已存在！"}, status=status.HTTP_200_OK)
            else:
                cur.execute(sql_2)
                conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request):
        cur = conn.cursor()

        hospital_id = request.query_params.get("hospital_id")

        sql_0 = "select doctor_phone from hb_doctors where hospital_id = '{}';".format(hospital_id)
        sql_1 = "update hb_hospitals set active = '1' where hospital_id = '{}';".format(hospital_id)
        sql_2 = "update hb_doctors set active = '1' where hospital_id = '{}';".format(hospital_id)
        sql_3 = "delete from hb_equipments where hospital_id = '{}';".format(hospital_id)
        sql_4 = "delete from hb_roles where phone in (select doctor_phone from hb_doctors where " \
                "hospital_id = '{}');".format(hospital_id)

        try:
            cur.execute(sql_0)
            doctor_phone_list = cur.fetchall()
            cur.execute(sql_1)
            cur.execute(sql_2)
            cur.execute(sql_3)
            cur.execute(sql_4)
            conn.commit()

            # 删除医生的redis缓存权限
            redis_conn = get_redis_connection("default")
            for doctor in doctor_phone_list:
                redis_conn.hdel(doctor[0], 'role', 'factory_id', 'permission')
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)


class HbHospitalListType(APIView):
    """医院列表  hb/hospital/list/{type}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Type):
        cur = conn.cursor()

        row = request.query_params.get('row', 10)
        page = request.query_params.get('page', 1)

        limit = int(row)
        offset = int(row) * (int(page) - 1)

        if Type == 'devices':
            sql = "select * from(select t1.hospital_id, t1.hospital_name, count(equipment_id), row_number() over " \
                  "(order by t1.time desc) as rn from hb_hospitals as t1 left join hb_equipments as t2 on " \
                  "t2.hospital_id = t1.hospital_id where t1.active = '0' group by t1.hospital_id)t where rn > {} " \
                  "limit {};".format(offset, limit)
        elif Type == 'doctors':
            sql = "select * from(select t1.hospital_id, t1.hospital_name, count(doctor_phone), row_number() over " \
                  "(order by t1.time desc) as rn from hb_hospitals as t1 left join hb_doctors as t2 on " \
                  "t2.hospital_id = t1.hospital_id and t2.active = '0' where t1.active = '0' group by " \
                  "t1.hospital_id)t where rn > {} limit {};".format(offset, limit)
        else:
            return Response({"res": 1, "errmsg": '路径参数类型有误'}, status=status.HTTP_200_OK)

        target = ['hospital_id', 'hospital_name', 'count']

        try:
            cur.execute(sql)
            result = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbDeviceBinding(APIView):
    """绑定设备 hb/device/binding"""
    permission_classes = [CommonAdminPermission]

    def post(self, request):
        cur = conn.cursor()

        hospital_id = request.data.get("hospital_id")
        device_id = request.data.get("device_id", list())
        Time = int(time.time())

        sql_0 = "select count(1) from hb_equipments where equipment_id = '{}';"
        sql_1 = "insert into hb_equipments(equipment_id, hospital_id, time) values('{}', '{}', {});"

        try:
            for i in device_id:
                cur.execute(sql_0.format(i))
                tmp = cur.fetchone()[0]
                if tmp == 0:
                    cur.execute(sql_1.format(i, hospital_id, Time))
                else:
                    return Response({'res': 1, 'errmsg': '所选设备中已有被绑定的'}, status=status.HTTP_200_OK)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)


class HbDeviceDelete(APIView):
    """删除设备 hb/device/delete/{id}"""
    permission_classes = [CommonAdminPermission]

    def delete(self, request, Id):
        cur = conn.cursor()

        # Id = request.data.get("id")

        sql = "delete from hb_equipments where equipment_id = '{}';".format(Id)

        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)


class HbDoctorsBinding(APIView):
    """绑定医生 hb/doctors/binding"""
    permission_classes = [CommonAdminPermission]

    def post(self, request):
        cur = conn.cursor()

        hospital_id = request.data.get("hospital_id")
        doctor_phone = request.data.get("doctor_phone", list())
        Time = int(time.time())

        invitor = request.redis_cache["username"]

        sql_0 = "select active from hb_doctors where doctor_phone = '{}';"
        sql_1 = "update hb_doctors set hospital_id = '{}', time = {}, active = '0' where doctor_phone = '{}';"
        sql_2 = "insert into hb_doctors(doctor_phone, hospital_id, time) values('{}', '{}', {});"
        sql_3 = "insert into hb_roles(phone, rights, time, invitor) values('{}', '{}', {}, '{}');"

        try:
            for i in doctor_phone:
                cur.execute(sql_0.format(i))
                tmp = cur.fetchone()
                if tmp is None:
                    cur.execute(sql_2.format(i, hospital_id, Time))
                elif tmp[0] == '1':
                    cur.execute(sql_1.format(hospital_id, Time, i))
                elif tmp[0] == '0':
                    return Response({'res': 0, 'errmsg': '所选医生中已有被绑定的'}, status=status.HTTP_200_OK)
                cur.execute(sql_3.format(i, 'doctor', Time, invitor))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)


class HbHospitalsMain(APIView):
    """医院总览主页 hb/hospitals/main"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        today_start = today_timestamp()[0]

        sql = "select t1.hospital_id, hospital_name, count(distinct t2.equipment_id), count(distinct t3.doctor_phone)" \
              ", count(distinct patient_id) from hb_hospitals t1 left join hb_equipments t2 on t1.hospital_id = " \
              "t2.hospital_id left join hb_doctors t3 on t1.hospital_id = t3.hospital_id and t3.active = '0' left " \
              "join hb_treatment_logs t4 on t1.hospital_id = t4.hospital_id and t4.time > {} where t1.active = '0' " \
              "group by t1.hospital_id;".format(today_start)

        target = ['hospital_id', 'hospital_name', 'devices_count', 'doctors_count', 'today_patients']

        try:
            cur.execute(sql)
            result = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbHospitalDetailId(APIView):
    """医院详情主页 hb/hospital/detail/{id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id):
        cur = conn.cursor()

        # 过去五天日期列表
        date_list = [arrow.now().shift(days=-i).format("MM.DD") for i in range(4, -1, -1)]
        sql_1 = "select to_char(TO_TIMESTAMP(time), 'MM.DD') as d, count(1) from hb_treatment_logs where " \
                "hospital_id = '{}' group by d order by d desc limit 5;".format(Id)
        try:
            cur.execute(sql_1)
            tmp = dict(cur.fetchall())
            recent_use = []
            for i in date_list:
                if i in tmp:
                    recent_use.append({'date': i, 'count': tmp[i]})
                else:
                    recent_use.append({'date': i, 'count': 0})
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        sql_2 = "select gender, count(gender) from hb_patients t1 left join hb_doctors t2 on t1.doctor_phone = " \
                "t2.doctor_phone where t2.hospital_id = '{}' group by gender;".format(Id)
        sql_3 = "select count(1) from hb_treatment_logs where hospital_id = '{}';".format(Id)
        try:
            cur.execute(sql_2)
            tmp = dict(cur.fetchall())
            male = tmp.get('0', 0)
            female = tmp.get('1', 0)
            patients_total = male + female
            cur.execute(sql_3)
            records_total = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 不同年龄段的患者
        sql_4 = "select sum(case when date_part('year', current_date) - date_part('year', birthday) < 19 then 1 else" \
                " 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday) between 19 and" \
                " 35 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday)" \
                " between 36 and 59 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part(" \
                "'year', birthday) > 59 then 1 else 0 end) from hb_patients t1 left join hb_doctors t2 on t1.doctor_" \
                "phone = t2.doctor_phone where t2.hospital_id = '{}';".format(Id)
        try:
            cur.execute(sql_4)
            tmp = cur.fetchone()
            patients_analysis = dict()
            patients_analysis['male'] = male
            patients_analysis['female'] = female
            patients_analysis['age_stage'] = [{'stage': str(i),
                                               'count': j,
                                               'percent': '{:.2f}%'.format(j/patients_total*100 if patients_total else 0)} for i,j in enumerate(tmp)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 医生总数，医生头像
        sql_5 = "select image from hb_doctors left join user_info on doctor_phone = phone where " \
                "hospital_id = '{}' and active = '0';".format(Id)
        try:
            alioss = AliOss()
            cur.execute(sql_5)
            tmp = cur.fetchall()
            doctors_total = len(tmp)
            images = [alioss.joint_image(i[0]) if not isinstance(i[0], memoryview)
                      else alioss.joint_image(i[0].tobytes().decode()) for i in tmp]

            doctors_detail = dict()
            doctors_detail['doctors_total'] = doctors_total
            doctors_detail['images'] = images
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 设备
        sql_6 = "select equipment_id, coalesce(type, '') as type, coalesce(name, '') as name, aval_times from " \
                "hb_equipments t1 left join hb_devices t2 on t1.equipment_id = t2.id where t1.hospital_id = '{}';".format(Id)
        sql_7 = "select count(distinct patient_id) from hb_treatment_logs where equipment_id = '{}' and time >= {} " \
                "and time < {};"
        target = ['device_id', 'device_category', 'device_name', 'left_usage']
        try:
            cur.execute(sql_6)
            devices_detail = [dict(zip(target, i)) for i in cur.fetchall()]
            today = today_timestamp()
            for i in devices_detail:
                cur.execute(sql_7.format(i['device_id'], today[0], today[1]))
                i['today_patients'] = cur.fetchone()[0]
                cur.execute(sql_7.format(i['device_id'], today[0]-86400, today[1]-86400))
                i['yesterday_patients'] = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['recent_use'] = recent_use
        result['patients_analysis'] = patients_analysis
        result['patients_total'] = patients_total
        result['records_total'] = records_total
        result['doctors_total'] = doctors_total
        result['images'] = images
        result['devices_detail'] = devices_detail

        return Response(result, status=status.HTTP_200_OK)


class HbDoctorsList(APIView):
    """医生列表 /hb/doctors/list"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        Id = request.query_params.get('hospital_id')
        # factory_id = 'hbyl'
        factory_id = request.redis_cache["factory_id"]
        row = request.query_params.get('row', 10)
        page = request.query_params.get('page', 1)

        limit = int(row)
        offset = int(row) * (int(page) - 1)

        # none代表所有未绑定医院的医生，id代表特定医院中的医生
        if Id == 'none':
            sql = "select * from(select t1.phone, coalesce(t2.name, '') as name, t2.image, row_number() over (order " \
                  "by t1.time desc) as rn from factory_users t1 left join user_info t2 on t1.phone = t2.phone left " \
                  "join hb_roles t3 on t1.phone = t3.phone where t3.phone is NULL and t1.factory = '{}')t where rn " \
                  "> {} limit {};".format(factory_id, offset, limit)
        else:
            sql = "select * from(select doctor_phone, coalesce(name, '') as name, image, row_number() over (order " \
                  "by t1.time desc) as rn from hb_doctors t1 left join user_info on t1.doctor_phone = phone where " \
                  "hospital_id = '{}' and active = '0')t where rn > {} limit {};".format(Id, offset, limit)
        target = ['phone', 'name', 'image']

        alioss = AliOss()
        try:
            cur.execute(sql)
            result = list()
            for i in cur.fetchall():
                tmp = dict(zip(target, i))
                if isinstance(tmp['image'], memoryview):
                    tmp['image'] = tmp['image'].tobytes().decode()
                tmp['image'] = alioss.joint_image(tmp['image'])
                result.append(tmp)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbHospitalDoctorId(APIView):
    """医生详情 hb/hospital/doctor/{id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id):
        cur = conn.cursor()

        condition = "and time > {} and time < {}"
        start_end = list()
        start_end.append(today_timestamp())
        start_end.append(week_timestamp())
        now = time.localtime()[:2]
        start_end.append(month_timestamp(now[0], now[1]))

        sql_1 = "select t1.doctor_phone, t1.id, coalesce(t2.name, '') as name, t2.image, coalesce(t5.name, ''), " \
                "t4.time, t3.hospital_name from hb_doctors t1 left join user_info t2 on t2.phone = t1.doctor_phone " \
                "left join hb_hospitals t3 on t3.hospital_id = t1.hospital_id left join hb_roles t4 on t4.phone = " \
                "t1.doctor_phone left join user_info t5 on t5.phone = t4.invitor where doctor_phone = '{}';".format(Id)
        sql_2 = "select count(1) from hb_treatment_logs where doctor_id = '{}' {};"
        sql_3 = "select count(1) from hb_patients where doctor_phone = '{}' {};"
        target = ['phone', 'id', 'name', 'image', 'invitor', 'invite_time', 'hospital_name']

        alioss = AliOss()
        try:
            cur.execute(sql_1)
            tmp = dict(zip(target, cur.fetchone()))
            if isinstance(tmp['image'], memoryview):
                tmp['image'] = tmp['image'].tobytes().decode()
            tmp['image'] = alioss.joint_image(tmp['image'])
            tmp['id'] = str(tmp['id']).zfill(6)

            treatment_stats = list()
            patients_stats = list()
            cur.execute(sql_2.format(Id, ''))
            treatment_stats.append(cur.fetchone()[0])
            cur.execute(sql_3.format(Id, ''))
            patients_stats.append(cur.fetchone()[0])
            for i in start_end:
                cur.execute(sql_2.format(Id, condition.format(i[0], i[1])))
                treatment_stats.append(cur.fetchone()[0])
                cur.execute(sql_3.format(Id, condition.format(i[0], i[1])))
                patients_stats.append(cur.fetchone()[0])
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        keys = ['total', 'day', 'week', 'month']
        treatment_stats = dict(zip(keys, treatment_stats))
        patients_stats = dict(zip(keys, patients_stats))

        result = dict()
        result.update(tmp)
        result['treatment_stats'] = treatment_stats
        result['patients_stats'] = patients_stats

        return Response(result, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        cur = conn.cursor()

        sql_0 = "update hb_doctors set active = '1' where doctor_phone = '{}';".format(Id)
        sql_1 = "delete from hb_roles where phone = '{}';".format(Id)
        try:
            cur.execute(sql_0)
            cur.execute(sql_1)
            conn.commit()

            # 删除医生的redis缓存权限
            redis_conn = get_redis_connection("default")
            redis_conn.hdel(Id, 'role', 'factory_id', 'permission')

        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'res': 0}, status=status.HTTP_200_OK)


class HbDeviceDetailId(APIView):
    """设备详情主页 hb/device/detail/{id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id):
        cur = conn.cursor()

        # 检查设备是否已经绑定医院
        sql = "select count(1) from hb_equipments where equipment_id = '{}';".format(Id)

        sql_0 = "select id, type, coalesce(name, '') as name, aval_times, totl_val from hb_devices where id = '{}';".format(Id)
        sql_1 = "select equipment_id, type, coalesce(name, '') as name, aval_times, totl_val, hospital_name from " \
                "hb_equipments t1 left join hb_devices t2 on t1.equipment_id = t2.id left join hb_hospitals t3 on " \
                "t1.hospital_id = t3.hospital_id where equipment_id = '{}';".format(Id)
        target = ['device_id', 'device_category', 'device_name', 'left_usage', 'total_usage', 'hospital_name']

        try:
            cur.execute(sql)
            tmp_0 = cur.fetchone()[0]
            if tmp_0 == 0:
                cur.execute(sql_0)
                tmp_1 = dict(zip(target, cur.fetchone()))
                tmp_1['hospital_name'] = ''
            else:
                cur.execute(sql_1)
                tmp_1 = dict(zip(target, cur.fetchone()))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 过去五天日期列表
        date_list = [arrow.now().shift(days=-i).format("MM.DD") for i in range(4, -1, -1)]
        sql_2 = "select to_char(TO_TIMESTAMP(time), 'MM.DD') as d, count(id) from hb_treatment_logs where " \
                "equipment_id = '{}' group by d order by d desc limit 5;".format(Id)
        try:
            cur.execute(sql_2)
            tmp_2 = dict(cur.fetchall())
            recent_use = []
            for i in date_list:
                if i in tmp_2:
                    recent_use.append({'date': i, 'count': tmp_2[i]})
                else:
                    recent_use.append({'date': i, 'count': 0})
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 医生排行
        sql_3 = "select t1.*, coalesce(t2.name, ''), t2.image from (select doctor_id, count(1) from hb_treatment_logs" \
                " t1 where equipment_id = '{}' group by doctor_id limit 3)t1 left join user_info t2 on t1.doctor_id " \
                "= t2.phone order by count desc".format(Id)
        target = ['phone', 'count', 'name', 'image']

        alioss = AliOss()
        try:
            cur.execute(sql_3)
            doctor_rank = [dict(zip(target, i)) for i in cur.fetchall()]
            for i in doctor_rank:
                if isinstance(i['image'], memoryview):
                    i['image'] = i['image'].tobytes().decode()
                i['image'] = alioss.joint_image(i['image'])
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 男女患者人数和不同年龄段的患者
        sql_4 = "select sum(case when t3.gender = '0' then 1 else 0 end ), sum(case when t3.gender = '1' then 1 else" \
                " 0 end) from (select distinct patient_id, gender from hb_treatment_logs t1, hb_patients t2 where " \
                "t1.patient_id = t2.patient_phone and t1.equipment_id = '{}')t3;".format(Id)
        sql_5 = "select sum(case when date_part('year', current_date) - date_part('year', birthday) < 19 then 1 else" \
                " 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday) between 19 and" \
                " 35 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday)" \
                " between 36 and 59 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part(" \
                "'year', birthday) > 59 then 1 else 0 end) from (select distinct patient_id, birthday from hb_treatment" \
                "_logs t1, hb_patients t2 where t1.patient_id = t2.patient_phone and t1.equipment_id = '{}')t3;".format(Id)
        try:
            cur.execute(sql_4)
            tmp_4 = cur.fetchone()
            cur.execute(sql_5)
            tmp_5 = cur.fetchone()
            if None in tmp_4:
                tmp_4 = (0, 0)
            if None in tmp_5:
                tmp_5 = (0, 0, 0, 0)

            patients_total = sum(tmp_4)
            patients_analysis = dict()
            patients_analysis['male'] = tmp_4[0]
            patients_analysis['female'] = tmp_4[1]
            patients_analysis['age_stage'] = [{'stage': str(i),
                                               'count': j,
                                               'percent': '{:.2f}%'.format(j/patients_total*100 if patients_total else 0)} for i,j in enumerate(tmp_5)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result.update(tmp_1)
        result['recent_use'] = recent_use
        result['doctor_rank'] = doctor_rank
        result['patients_analysis'] = patients_analysis

        return Response(result, status=status.HTTP_200_OK)


class HbPatientsAnalysisId(APIView):
    """患者数据分析 hb/patients/analysis/{device_id}/{hospital_id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, device_id, hospital_id):
        cur = conn.cursor()

        Type = request.query_params.get('type', 'daily')
        date = request.query_params.get('date')

        # 默认值
        if Type == 'daily' and not date:
            date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        elif Type == 'monthly' and not date:
            date = time.strftime('%Y-%m', time.localtime(time.time()))
        elif Type == 'weekly' and not date:
            # week_date = '%d-%d'.format(arrow.now().isocalendar()[0], arrow.now().isocalendar()[1])
            start_timestamp = week_timestamp()[0]

        # 根据设备id和医院id生成条件
        if device_id == 'all' and hospital_id == 'all':
            condition_1 = ""
        elif device_id == 'all' and hospital_id != 'all':
            condition_1 = "t1.hospital_id = '{}' and".format(hospital_id)
        elif hospital_id == 'all':
            condition_1 = "t1.equipment_id = '{}' and".format(device_id)
        else:
            condition_1 = "t1.equipment_id = '{}' and t1.hospital_id = '{}' and".format(device_id, hospital_id)

        # 起始日期时间戳
        if Type == 'daily':
            year, month, day = date.split('-')
            start_timestamp, end_timestamp = someday_timestamp(int(year), int(month), int(day))
        elif Type == 'weekly':
            # 修改为统一的isocalendar周数
            # week_date = datetime.datetime.strptime(date, '%G-%V-%u')
            # start_timestamp = time.mktime(week_date.timetuple())
            # end_timestamp = start_timestamp + 7 * 86400
            if date:
                year, month, week = date.split('-')
                month_list = calendar.monthcalendar(int(year), int(month))
                if month_list[0][0] == 1:
                    day = month_list[int(week) - 1][0]
                else:
                    day = month_list[int(week)][0]
                week_date = '%s-%s-%d' % (year, month, day)
                start_timestamp = time.mktime(time.strptime(week_date, '%Y-%m-%d'))
            end_timestamp = start_timestamp + 7 * 86400
        elif Type == 'monthly':
            year, month = date.split('-')
            start_timestamp = time.mktime(time.strptime(date, '%Y-%m'))
            year, month = correct_time(int(year), int(month) + 1)
            end_timestamp = time.mktime(time.strptime('%s-%s' % (year, month), '%Y-%m'))

        condition_2 = "t1.time >= {} and t1.time < {}".format(start_timestamp, end_timestamp)

        # 男女患者人数和不同年龄段的患者
        sql_1 = "select sum(case when t3.gender = '0' then 1 else 0 end ), sum(case when t3.gender = '1' then 1 else" \
                " 0 end) from (select distinct patient_id, gender from hb_treatment_logs t1, hb_patients t2 where " \
                "t1.patient_id = t2.patient_phone and {} {})t3;".format(condition_1, condition_2)
        sql_2 = "select sum(case when date_part('year', current_date) - date_part('year', birthday) < 19 then 1 else" \
                " 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday) between 19 and" \
                " 35 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday)" \
                " between 36 and 59 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part(" \
                "'year', birthday) > 59 then 1 else 0 end) from (select distinct patient_id, birthday from hb_treatment" \
                "_logs t1, hb_patients t2 where t1.patient_id = t2.patient_phone and {} {})t3" \
                ";".format(condition_1, condition_2)
        try:
            cur.execute(sql_1)
            tmp_4 = cur.fetchone()
            cur.execute(sql_2)
            tmp_5 = cur.fetchone()
            if None in tmp_4:
                tmp_4 = (0, 0)
            if None in tmp_5:
                tmp_5 = (0, 0, 0, 0)

            patients_total = sum(tmp_4)
            patients_analysis = dict()
            patients_analysis['male'] = tmp_4[0]
            patients_analysis['female'] = tmp_4[1]
            patients_analysis['age_stage'] = [{'stage': str(i),
                                               'count': j,
                                               'percent': '{:.2f}%'.format(j/patients_total*100 if patients_total else 0)} for i,j in enumerate(tmp_5)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 过去四天/周/月时间列表
        if Type == 'daily':
            date_type = '前日'
            time_type = 'MM月DD日'
            date_list = [arrow.get(date, 'YYYY-MM-DD').shift(days=-i).format("MM月DD日") for i in range(3, -1, -1)]
        elif Type == 'monthly':
            date_type = '前月'
            time_type = 'MM月'
            date_list = [arrow.get(date, 'YYYY-MM').shift(months=-i).format("MM月") for i in range(3, -1, -1)]
        elif Type == 'weekly':
            date_type = '前周'
            date_list = [(start_timestamp - i * 7 * 86400, end_timestamp - i * 7 * 86400) for i in range(3, -1, -1)]

        # 患者增减对比
        sql_3 = "select to_char(TO_TIMESTAMP(time), '{}') as d, count(distinct patient_id) from hb_treatment_logs t1" \
                " where {} t1.time < {} group by d order by d desc limit 4;"
        sql_4 = "select count(distinct patient_id) from hb_treatment_logs t1 where {} t1.time >= " \
                "{} and t1.time < {};"
        try:
            recent_patients = []
            if Type != 'weekly':
                cur.execute(sql_3.format(time_type, condition_1, end_timestamp))
                tmp = dict(cur.fetchall())
                for i in date_list:
                    if i in tmp:
                        recent_patients.append({'date': i, 'count': tmp[i]})
                    else:
                        recent_patients.append({'date': i, 'count': 0})
            else:
                for i in date_list:
                    date_start = datetime.datetime.fromtimestamp(i[0])
                    date_end = datetime.datetime.fromtimestamp(i[1])
                    cur.execute(sql_4.format(condition_1, i[0], i[1]))
                    weekly_count = cur.fetchone()[0]
                    week_date = '%s-%s' % (date_start.strftime('%m.%d'), date_end.strftime('%m.%d'))
                    # recent_patients.append({'date': week_date, 'count': 0})  # BUG: 按周查询取结果, value不应为0
                    recent_patients.append({'date': week_date, 'count': weekly_count})

            # BUG: 当前日/周/月 与 昨日/上周/上个月 相比, 取索引错误
            # patients_change = {'date': recent_patients[-1]['date'], 'type': date_type,
            #                    'change': recent_patients[-1]['count'] - recent_patients[1]['count']}
            patients_change = {'date': recent_patients[-1]['date'], 'type': date_type,
                               'change': recent_patients[-1]['count'] - recent_patients[-2]['count']}
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 治疗部位top10
        sql_5 = "select unnest(body_parts) as body_part, count(1) from hb_treatment_logs t1 where {} {} group by " \
                "body_part order by count desc limit 10;".format(condition_1, condition_2)
        sql_6 = "select sum(count) from (select unnest(body_parts) as body_part, count(1) from hb_treatment_logs t1 " \
                "where {} {} group by body_part)t ;".format(condition_1, condition_2)
        try:
            cur.execute(sql_6)
            total = cur.fetchone()[0]
            cur.execute(sql_5)
            treatment_part = list()
            for i in cur.fetchall():
                tmp = dict()
                tmp['body_part'] = i[0]
                tmp['count'] = i[1]
                tmp['percent'] = '{:.2f}%'.format(i[1] / total * 100 if total else 0)
                treatment_part.append(tmp)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['patients_analysis'] = patients_analysis
        result['recent_patients'] = recent_patients
        result['treatment_part'] = treatment_part
        result['patients_change'] = patients_change

        return Response(result, status=status.HTTP_200_OK)


class HbPatientsCalendarIdType(APIView):
    """治疗患者日历统计 hb/patient/calendar/{device_id}/{hospital_id}"""
    """医生排行日历统计 hb/doctor/calendar/{device_id}/{hospital_id}"""
    '''此API疑似被废弃, 找不到对应的界面'''
    permission_classes = [CommonAdminPermission]

    def get(self, request, device_id, hospital_id):
        cur = conn.cursor()

        Type = request.query_params.get('type', 'daily')

        # 根据设备id和医院id生成条件
        if device_id == 'all' and hospital_id == 'all':
            condition = ""
        elif device_id == 'all' and hospital_id != 'all':
            condition = "and hospital_id = '{}'".format(hospital_id)
        elif hospital_id == 'all':
            condition = "and equipment_id = '{}'".format(device_id)
        else:
            condition = "and equipment_id = '{}' and hospital_id = '{}'".format(device_id, hospital_id)

        if Type == 'daily':
            sql = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as d, count(distinct patient_id) from hb_treatment" \
                  "_logs where time >= {} {} group by d order by d desc;"
        elif Type == 'monthly':
            sql = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM') as d, count(distinct patient_id) from hb_treatment" \
                  "_logs where time >= {} {} group by d order by d desc limit 6;"
        elif Type == 'weekly':
            sql = "select count(distinct patient_id) from hb_treatment_logs where time >= {} and time < {} %s;" % condition
        else:
            return Response({"res": 1, "errmsg": '日期类型参数有误'}, status=status.HTTP_200_OK)
        target = ['date', 'count']

        result = []
        if Type == 'weekly':
            '''BUG: 按周查询返回数据格式有问题'''
            start_timestamp = week_timestamp()[1]
            tmp = list()
            result = list()
            times = 1
            while len(tmp) <= 6:
                start_timestamp -= 7 * 86400
                date = '%d-%s' % (time.localtime(start_timestamp)[0], str(time.localtime(start_timestamp)[1]).zfill(2))
                if date in tmp:
                    times += 1
                    cur.execute(sql.format(start_timestamp, start_timestamp + 7 * 86400))
                    result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0]})
                elif len(tmp) < 6:
                    times = 1
                    cur.execute(sql.format(start_timestamp, start_timestamp + 7 * 86400))
                    result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0]})
                    tmp.append(date)
                else:
                    break
        elif Type == 'daily' or Type == 'monthly':
            year, month = time.localtime()[:2]
            start_year, start_month = correct_time(year, month - 5)
            start_timestamp = time.mktime(time.strptime('%d-%d-01' % (start_year, start_month), '%Y-%m-%d'))
            cur.execute(sql.format(start_timestamp, condition))
            result = [dict(zip(target, i)) for i in cur.fetchall()]

        return Response(result, status=status.HTTP_200_OK)


class HbDeviceHospitalRankId(APIView):
    """医院排行榜 hb/device/hospital/rank/{id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id):
        cur = conn.cursor()

        Type = request.query_params.get('type', 'daily')
        date = request.query_params.get('date')
        row = request.query_params.get('row', 10)
        page = request.query_params.get('page', 1)

        limit = int(row)
        offset = int(row) * (int(page) - 1)

        # 根据id生成条件
        if Id == 'all':
            condition = " "
        else:
            condition = " equipment_id = '{}' and ".format(Id)

        # 默认值
        if Type == 'daily' and not date:
            date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        elif Type == 'monthly' and not date:
            date = time.strftime('%Y-%m', time.localtime(time.time()))
        elif Type == 'weekly' and not date:
            start_timestamp = week_timestamp()[0]

        # 起始时间戳
        if Type == 'daily':
            year, month, day = date.split('-')
            start_timestamp, end_timestamp = someday_timestamp(int(year), int(month), int(day))
        elif Type == 'weekly':
            if date:
                year, month, week = date.split('-')
                month_list = calendar.monthcalendar(int(year), int(month))
                if month_list[0][0] == 1:
                    day = month_list[int(week) - 1][0]
                else:
                    day = month_list[int(week)][0]
                week_date = '%s-%s-%d' % (year, month, day)
                start_timestamp = time.mktime(time.strptime(week_date, '%Y-%m-%d'))
            end_timestamp = start_timestamp + 7 * 86400
        elif Type == 'monthly':
            year, month = date.split('-')
            start_timestamp = time.mktime(time.strptime(date, '%Y-%m'))
            year, month = correct_time(int(year), int(month) + 1)
            end_timestamp = time.mktime(time.strptime('%s-%s' % (year, month), '%Y-%m'))
        else:
            return Response({"res": 1, "errmsg": "日期类型参数有误"}, status=status.HTTP_200_OK)

        sql = "select * from(select t1.hospital_id, coalesce(t2.hospital_name, '') as name, count, row_number() " \
              "over (order by count desc) as rn from (select hospital_id, count(1) from hb_treatment_logs where{}" \
              "time >= {} and time < {} group by hospital_id)t1 left join hb_hospitals t2 on t1.hospital_id = " \
              "t2.hospital_id)t where rn > {} limit {};".format(condition, start_timestamp, end_timestamp, offset, limit)
        target = ['id', 'name', 'count']

        try:
            cur.execute(sql)
            result = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbDeviceHospitalCalendarIdType(APIView):
    """医院排行日历统计 hb/device/hospital/calendar/{id}/{type}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id, Type):
        cur = conn.cursor()

        if Id == 'all':
            condition_1 = ""
        else:
            condition_1 = "and equipment_id = '{}'".format(Id)

        if Type == 'daily':
            sql = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM-DD') as d, count(1) from hb_treatment_logs where " \
                  "time >= {} {} group by d order by d desc;"
        elif Type == 'monthly':
            sql = "select to_char(TO_TIMESTAMP(time), 'YYYY-MM') as d, count(1) from hb_treatment_logs where time " \
                  ">= {} {} group by d order by d desc limit 6;"
        elif Type == 'weekly':
            sql = "select count(1) from hb_treatment_logs where time >= {} and time < {} %s;" % condition_1
        else:
            return Response({"res": 1, "errmsg": '日期类型参数有误'}, status=status.HTTP_200_OK)
        target = ['date', 'count']

        result = []
        if Type == 'weekly':
            start_timestamp = week_timestamp()[1]
            tmp = list()
            result = list()
            times = 1
            while len(tmp) <= 6:
                start_timestamp -= 7 * 86400
                date = '%d-%s' % (time.localtime(start_timestamp)[0], str(time.localtime(start_timestamp)[1]).zfill(2))

                if date in tmp:
                    times += 1
                    cur.execute(sql.format(start_timestamp, start_timestamp + 7 * 86400))
                    date_start = datetime.datetime.fromtimestamp(start_timestamp)
                    date_end = date_start - datetime.timedelta(days=-6)
                    result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0],
                                   'week_date': '%s-%s' % (date_start.strftime('%m.%d'), date_end.strftime('%m.%d'))})
                    # result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0]})
                elif len(tmp) < 6:
                    times = 1
                    cur.execute(sql.format(start_timestamp, start_timestamp + 7 * 86400))
                    date_start = datetime.datetime.fromtimestamp(start_timestamp)
                    date_end = date_start - datetime.timedelta(days=-6)
                    result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0],
                                   'week_date': '%s-%s' % (date_start.strftime('%m.%d'), date_end.strftime('%m.%d'))})
                    # result.append({'date': date + '-' + str(times).zfill(2), 'count': cur.fetchone()[0]})
                    tmp.append(date)
                else:
                    break
        elif Type == 'daily' or Type == 'monthly':
            year, month = time.localtime()[:2]
            start_year, start_month = correct_time(year, month - 5)
            start_timestamp = time.mktime(time.strptime('%d-%d-01' % (start_year, start_month), '%Y-%m-%d'))
            cur.execute(sql.format(start_timestamp, condition_1))
            result = [dict(zip(target, i)) for i in cur.fetchall()]

        return Response(result, status=status.HTTP_200_OK)


class HbDoctorRankId(APIView):
    """医生排行榜 hb/doctor/rank/{device_id}/{hospital_id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, device_id, hospital_id):
        cur = conn.cursor()

        Type = request.query_params.get('type', 'daily')
        date = request.query_params.get('date')
        row = request.query_params.get('row', 10)
        page = request.query_params.get('page', 1)

        limit = int(row)
        offset = int(row) * (int(page) - 1)

        # 默认值
        if Type == 'daily' and not date:
            date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        elif Type == 'monthly' and not date:
            date = time.strftime('%Y-%m', time.localtime(time.time()))
        elif Type == 'weekly' and not date:
            start_timestamp = week_timestamp()[0]

        # 根据设备id和医院id生成条件
        if device_id == 'all' and hospital_id == 'all':
            condition = ""
        elif device_id == 'all' and hospital_id != 'all':
            condition = "hospital_id = '{}' and".format(hospital_id)
        elif hospital_id == 'all':
            condition = "equipment_id = '{}' and".format(device_id)
        else:
            condition = "equipment_id = '{}' and hospital_id = '{}' and".format(device_id, hospital_id)

        # 起始时间戳
        if Type == 'daily':
            year, month, day = date.split('-')
            start_timestamp, end_timestamp = someday_timestamp(int(year), int(month), int(day))
        elif Type == 'weekly':
            if date:
                year, month, week = date.split('-')
                month_list = calendar.monthcalendar(int(year), int(month))
                if month_list[0][0] == 1:
                    day = month_list[int(week) - 1][0]
                else:
                    day = month_list[int(week)][0]
                week_date = '%s-%s-%d' % (year, month, day)
                start_timestamp = time.mktime(time.strptime(week_date, '%Y-%m-%d'))
            end_timestamp = start_timestamp + 7 * 86400
        elif Type == 'monthly':
            year, month = date.split('-')
            start_timestamp = time.mktime(time.strptime(date, '%Y-%m'))
            year, month = correct_time(int(year), int(month) + 1)
            end_timestamp = time.mktime(time.strptime('%s-%s' % (year, month), '%Y-%m'))
        else:
            return Response({"res": 1, "errmsg": "日期类型参数有误"}, status=status.HTTP_200_OK)

        sql = "select * from(select doctor_id, coalesce(t2.name, '') as name, count, t2.image, row_number() over " \
              "(order by count desc) as rn from(select doctor_id, count(distinct patient_id) from hb_treatment_logs" \
              " where {} time >= {} and time < {} group by doctor_id)t1 left join user_info t2 on t1.doctor_id = " \
              "t2.phone)t where rn > {} limit {};".format(condition, start_timestamp, end_timestamp, offset, limit)
        target = ['id', 'name', 'count', 'image']

        try:
            cur.execute(sql)
            result = [dict(zip(target, i)) for i in cur.fetchall()]
            alioss = AliOss()
            for i in result:
                if 'image' in i:
                    if isinstance(i['image'], memoryview):
                        i['image'] = i['image'].tobytes().decode()
                    i['image'] = alioss.joint_image(i['image'])
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbDevicesMain(APIView):
    """设备总览主页 hb/devices/main"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        # 过去五天使用列表
        date_list = [arrow.now().shift(days=-i).format("MM.DD") for i in range(4, -1, -1)]
        sql_1 = "select to_char(TO_TIMESTAMP(time), 'MM.DD') as d, count(1) from hb_treatment_logs group by d " \
                "order by d desc limit 5;"
        try:
            cur.execute(sql_1)
            tmp = dict(cur.fetchall())
            recent_use = []
            for i in date_list:
                if i in tmp:
                    recent_use.append({'date': i, 'count': tmp[i]})
                else:
                    recent_use.append({'date': i, 'count': 0})
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 医院排行
        sql_3 = "select t1.*, coalesce(t2.hospital_name, '') from (select hospital_id, count(1) from " \
                "hb_treatment_logs t1 group by hospital_id limit 3)t1 left join hb_hospitals t2 on t1.hospital_id =" \
                " t2.hospital_id order by count desc;"
        target = ['id', 'count', 'name']

        try:
            cur.execute(sql_3)
            hospital_rank = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 男女患者人数和不同年龄段的患者
        sql_4 = "select sum(case when t3.gender = '0' then 1 else 0 end ), sum(case when t3.gender = '1' then 1 else" \
                " 0 end) from (select distinct patient_id, gender from hb_treatment_logs t1, hb_patients t2 where " \
                "t1.patient_id = t2.patient_phone )t3;"
        sql_5 = "select sum(case when date_part('year', current_date) - date_part('year', birthday) < 19 then 1 else" \
                " 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday) between 19 and" \
                " 35 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part('year', birthday)" \
                " between 36 and 59 then 1 else 0 end), sum(case when date_part('year', current_date) - date_part(" \
                "'year', birthday) > 59 then 1 else 0 end) from (select distinct patient_id, birthday from hb_treatment" \
                "_logs t1, hb_patients t2 where t1.patient_id = t2.patient_phone )t3;"
        try:
            cur.execute(sql_4)
            tmp_4 = cur.fetchone()
            cur.execute(sql_5)
            tmp_5 = cur.fetchone()
            if None in tmp_4:
                tmp_4 = (0, 0)
            if None in tmp_5:
                tmp_5 = (0, 0, 0, 0)

            patients_total = sum(tmp_4)
            patients_analysis = dict()
            patients_analysis['male'] = tmp_4[0]
            patients_analysis['female'] = tmp_4[1]
            patients_analysis['age_stage'] = [{'stage': str(i),
                                               'count': j,
                                               'percent': '{:.2f}%'.format(j/patients_total*100 if patients_total else 0)} for i,j in enumerate(tmp_5)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['recent_use'] = recent_use
        result['hospital_rank'] = hospital_rank
        result['patients_analysis'] = patients_analysis

        return Response(result, status=status.HTTP_200_OK)


class HbDevicesList(APIView):
    """设备列表 hb/devices/list"""
    permission_classes = [AllPermission]

    def get(self, request):
        cur = conn.cursor()

        # 0: 未绑定医院的设备列表，返回一个数组
        # 1: 全部的设备列表，按医院分类
        # hospital_id: 某家医院的设备列表
        Type = request.query_params.get('type')

        sql_1 = "select id, type, coalesce(name, '') as name, aval_times, totl_val from hb_devices t1 left join " \
                "hb_equipments t2 on t1.id = t2.equipment_id where t2.hospital_id is NULL;"
        sql_2 = "select t2.hospital_name, t1.equipment_id, t3.type, coalesce(t3.name, '') as name, t3.aval_times, " \
                "t3.totl_val from hb_equipments t1 left join hb_hospitals t2 on t1.hospital_id = t2.hospital_id left" \
                " join hb_devices t3 on t1.equipment_id = t3.id;"
        sql_3 = "select id, type, coalesce(name, '') as name, aval_times, totl_val from hb_devices t1 left join " \
                "hb_equipments t2 on t1.id = t2.equipment_id where t2.hospital_id = '{}';"
        target_1 = ['device_id', 'device_category', 'device_name', 'left_usage', 'total_usage']
        target_2 = ['hospital_name', 'device_id', 'device_category', 'device_name', 'left_usage', 'total_usage']

        try:
            if Type == '0':
                cur.execute(sql_1)
                result = [dict(zip(target_1, i)) for i in cur.fetchall()]
            elif Type == '1':
                cur.execute(sql_1)
                out_hospital = [dict(zip(target_1, i)) for i in cur.fetchall()]
                cur.execute(sql_2)

                tmp = {}
                for i in cur.fetchall():
                    if i[0] in tmp:
                        tmp[i[0]].append(dict(zip(target_2, i)))
                    else:
                        tmp[i[0]] = [dict(zip(target_2, i))]

                in_hospital = list()
                for i in tmp:
                    in_hospital.append({'hospital_name': i, 'list': tmp[i]})

                result = dict()
                result['out_hospital'] = out_hospital
                result['in_hospital'] = in_hospital
            else:
                cur.execute(sql_3.format(Type))
                result = [dict(zip(target_1, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbBossMain(APIView):
    """治疗机老板主页 hb/boss/main"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        sql_1 = "select count(1) from hb_devices union all select count(1) from hb_equipments union all select " \
                "count(1) from hb_doctors where active = '0' union all select count(1) from hb_hospitals where " \
                "active = '0' union all select count(1) from hb_patients t1 left join hb_doctors t2 on " \
                "t1.doctor_phone = t2.doctor_phone where t2.active = '0';"
        sql_2 = "select count(distinct patient_id) from hb_treatment_logs where time >= {} and time < {};"

        # 获取时间戳
        day_start_timestamp, day_end_timestamp = today_timestamp()
        month_start_timestamp, month_end_timestamp = month_timestamp(time.localtime()[0], time.localtime()[1])

        try:
            cur.execute(sql_1)
            tmp = cur.fetchall()

            cur.execute(sql_2.format(day_start_timestamp, day_end_timestamp))
            today = cur.fetchone()[0]
            cur.execute(sql_2.format(day_start_timestamp - 86400, day_end_timestamp - 86400))
            yesterday = cur.fetchone()[0]
            cur.execute(sql_2.format(month_start_timestamp, month_end_timestamp))
            month = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['devices_summary'] = {'devices_total': tmp[0][0], 'today': today, 'yesterday': yesterday}
        result['hospitals_summary'] = {'devices_total': tmp[1][0], 'hospitals_total': tmp[3][0], 'doctors_total': tmp[2][0]}
        result['patients_summary'] = {'patients_total': tmp[4][0], 'per_hospital': round(tmp[4][0] / tmp[3][0]) if tmp[1][0] else 0,
                                      'per_device': round(tmp[4][0] / tmp[0][0]) if tmp[0][0] else 0, 'month': month}

        return Response(result, status=status.HTTP_200_OK)


class HbRechargeList(APIView):
    """充值记录 hb/recharge/list"""
    permission_classes = [CommonAdminPermission]

    def get(self, request):
        cur = conn.cursor()

        row = request.query_params.get('row', 10)
        page = request.query_params.get('page', 1)

        limit = int(row)
        offset = int(row) * (int(page) - 1)

        sql = "select * from(select t1.id, recharge_counts, state, t3.hospital_name, coalesce(t4.name, '') as name, " \
              "t4.image, row_number() over (order by t1.time desc) as rn from hb_recharge_logs t1 left join " \
              "hb_equipments t2 on t1.equipment_id = t2.equipment_id left join hb_hospitals t3 on t2.hospital_id = " \
              "t3.hospital_id left join user_info t4 on t1.user_phone = t4.phone)t where rn > {} order by rn asc " \
              "limit {};".format(offset, limit)

        target = ['id', 'recharge_counts', 'state', 'hospital_name', 'user_name', 'user_image']

        try:
            alioss = AliOss()
            cur.execute(sql)
            result = [dict(zip(target, i)) for i in cur.fetchall()]
            for i in result:
                if isinstance(i['user_image'], memoryview):
                    i['user_image'] = i['user_image'].tobytes().decode()
                i['user_image'] = alioss.joint_image(i['user_image'])
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)


class HbRechargeDetailId(APIView):
    """充值记录详情 hb/recharge/detail/{id}"""
    permission_classes = [CommonAdminPermission]

    def get(self, request, Id):
        cur = conn.cursor()

        sql = "select t1.equipment_id, recharge_counts, user_phone, state, t1.time, t3.hospital_name, " \
              "coalesce(t4.name, '') as user_name, t4.image, coalesce(t5.name, '') as equipment_name from " \
              "hb_recharge_logs t1 left join hb_equipments t2 on t1.equipment_id = t2.equipment_id left join " \
              "hb_hospitals t3 on t2.hospital_id = t3.hospital_id left join user_info t4 on t1.user_phone = t4.phone " \
              "left join hb_devices t5 on t1.equipment_id = t5.id where t1.id = '{}';".format(Id)

        target = ['device_id', 'recharge_counts', 'user_phone', 'state', 'time', 'hospital_name', 'user_name',
                  'user_image', 'equipment_name']

        try:
            alioss = AliOss()
            cur.execute(sql)
            result = dict(zip(target, cur.fetchone()))
            if isinstance(result['user_image'], memoryview):
                result['user_image'] = result['user_image'].tobytes().decode()
            result['user_image'] = alioss.joint_image(result['user_image'])
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器异常"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(result, status=status.HTTP_200_OK)
