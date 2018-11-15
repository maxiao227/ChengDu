# -*- coding: utf-8 -*-
import time
from concurrent.futures import ThreadPoolExecutor

import schedule
import yaml

from CongestionRank import CongestionRank
from IndexofTheTraffic import IndexofTheTraffic

executor = ThreadPoolExecutor(2)


def indexofthetrafficcontrolControl():
    """
    交通指数
    """
    indexofthetraffic = IndexofTheTraffic(db_info)
    indexofthetraffic.deal()


def CongestionRankControl():
    """
    拥堵排名
    """
    congestionrank = CongestionRank(db_info)
    congestionrank.deal()


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


if __name__ == '__main__':
    """
    Main函数
    """
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']

    schedule.every().minutes.do(job1_task)
    schedule.every().minutes.do(job2_task)
    while True:
        schedule.run_pending()
        time.sleep(1)
