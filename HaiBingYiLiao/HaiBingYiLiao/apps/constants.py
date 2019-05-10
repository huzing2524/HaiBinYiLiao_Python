# -*- coding: utf-8 -*-
# @Time   : 19-3-19 上午11:01
# @Author : huziying
# @File   : constants.py

BG_QUEUE_NAME = "HB-ERL-Backend"  # RabbitMQ routing_key

REDIS_CACHE = {'username': '',
               'factory_id': '',
               'permission': ''}

RIGHTS_DICT = {
    "1": "超级管理员",
    "2": "高级管理员",
    "3": "市场部",
    "4": "财务部",
    "5": "采购部",
    "6": "客户管理",
    "7": "生产部",
    "8": "权限管理",
    "9": "仓库部",
    "10": "治疗仪入口",
    "11": "治疗仪管理"
}

ROW = 10  # 翻页时每页的数量
