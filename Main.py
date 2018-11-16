# -*- coding: utf-8 -*-
import time
from concurrent.futures import ThreadPoolExecutor

import schedule
import yaml

from CongestionRank import CongestionRank
from IndexofTheTraffic import IndexofTheTraffic
from TrafficMileage import TrafficMileage

executor = ThreadPoolExecutor(2)


def indexofthetrafficcontrolControl():
    """
    交通指数
    """
    indexofthetraffic = IndexofTheTraffic(db_info)
    result = indexofthetraffic.deal()
    if result:
        print(u'交通指数算法执行成功')
    else:
        indexofthetraffic.planB()


def CongestionRankControl():
    """
    拥堵排名
    """
    congestionrank = CongestionRank(db_info)
    result = congestionrank.deal()
    if result:
        print(u'拥堵排名算法执行成功')
    else:
        congestionrank.planB()


def TrafficMileageControl():
    """
    拥堵里程
    """
    trafficmileage = TrafficMileage(db_info)
    result = trafficmileage.deal()
    if result:
        print(u'拥堵里程算法执行成功')
    else:
        trafficmileage.planB()


def job1_task():
    """
    将交通指数放入线程池中执行
    """
    # threading.Thread(target=indexofthetrafficcontrolControl).start()
    executor.submit(indexofthetrafficcontrolControl)


def job2_task():
    """
    将拥堵排名放入线程池中执行
    """
    # threading.Thread(target=CongestionRankControl).start()
    executor.submit(CongestionRankControl)


def job3_task():
    """
    将拥堵里程放入线程池中执行
    """
    # threading.Thread(target=TrafficMileageControl).start()
    executor.submit(TrafficMileageControl)


if __name__ == '__main__':
    """
    Main函数
    """
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']

    schedule.every(2).minutes.do(job1_task)
    schedule.every(2).minutes.do(job2_task)
    schedule.every(2).minutes.do(job3_task)
    while True:
        schedule.run_pending()
        time.sleep(1)
