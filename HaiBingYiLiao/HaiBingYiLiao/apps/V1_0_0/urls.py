# -*- coding: utf-8 -*-
# @Time   : 19-3-19 上午11:10
# @Author : huziying
# @File   : urls.py

from django.conf.urls import url
from V1_0_0 import hzy_views, jcj_views

urlpatterns = [
    url(r"^generate/token", hzy_views.GenerateToken.as_view()),
    url(r"^hb/rights/info", hzy_views.RightsInfo.as_view()),
    url(r"^hb/manager", hzy_views.HbManager.as_view()),
    url(r"^hb/user/list", hzy_views.UserList.as_view()),
    url(r"^rights/orgs", hzy_views.RightsOrg.as_view()),
    url(r"^hb/patients/main", hzy_views.PatientsMain.as_view()),
    url(r"^hb/patients/statistics/(\w+)", hzy_views.PatientsStatistics.as_view()),
    url(r"^hb/patients/list", hzy_views.PatientsList.as_view()),
    url(r"^hb/treatment/records", hzy_views.PatientsTreatmentRecords.as_view()),
    url(r"^hb/patients/detail/(\w+)", hzy_views.PatientDetail.as_view()),
    url(r"^hb/doctor/main", hzy_views.DoctorMain.as_view()),
    url(r"^hb/doctor/manager", hzy_views.DoctorHbManager.as_view()),
    url(r"^hb/doctor/patients/new", hzy_views.PatientNew.as_view()),
    url(r"^hb/doctor/records/new", hzy_views.DoctorTreatmentRecordNew.as_view()),
    url(r"^hb/doctor/patients/list", hzy_views.DoctorPatientsList.as_view()),
    url(r"^hb/doctor/treatment/records", hzy_views.DoctorTreatmentRecords.as_view()),
    url(r"^hb/recharge/new", hzy_views.EquipmentRecharge.as_view()),

    # jcj_views-------------------------------------------------------------------------------------------------------
    url(r"^hb/hospital/doctor/(\w+)", jcj_views.HbHospitalDoctorId.as_view()),
    url(r"^hb/hospital/list/(\w+)", jcj_views.HbHospitalListType.as_view()),
    url(r"^hb/hospital/detail/(\w+)", jcj_views.HbHospitalDetailId.as_view()),
    url(r"^hb/hospitals/main", jcj_views.HbHospitalsMain.as_view()),
    url(r"^hb/hospital", jcj_views.HbHospital.as_view()),
    url(r"^hb/device/binding", jcj_views.HbDeviceBinding.as_view()),
    url(r"^hb/device/delete/(\w+)", jcj_views.HbDeviceDelete.as_view()),
    url(r"^hb/devices/main", jcj_views.HbDevicesMain.as_view()),
    url(r"^hb/device/detail/(\w+)", jcj_views.HbDeviceDetailId.as_view()),
    url(r"^hb/device/hospital/rank/(\w+)", jcj_views.HbDeviceHospitalRankId.as_view()),
    url(r"^hb/device/hospital/calendar/(\w+)/(\w+)", jcj_views.HbDeviceHospitalCalendarIdType.as_view()),
    url(r"^hb/doctors/binding", jcj_views.HbDoctorsBinding.as_view()),
    url(r"^hb/doctors/list", jcj_views.HbDoctorsList.as_view()),
    url(r"^hb/doctor/rank/(\w+)/(\w+)", jcj_views.HbDoctorRankId.as_view()),
    url(r"^hb/doctor/calendar/(\w+)/(\w+)", jcj_views.HbPatientsCalendarIdType.as_view()),
    url(r"^hb/patients/analysis/(\w+)/(\w+)", jcj_views.HbPatientsAnalysisId.as_view()),
    url(r"^hb/patient/calendar/(\w+)/(\w+)", jcj_views.HbPatientsCalendarIdType.as_view()),
    url(r"^hb/devices/list", jcj_views.HbDevicesList.as_view()),
    url(r"^hb/boss/main", jcj_views.HbBossMain.as_view()),
    url(r"^hb/recharge/list", jcj_views.HbRechargeList.as_view()),
    url(r"^hb/recharge/detail/(\w+)", jcj_views.HbRechargeDetailId.as_view()),
]
