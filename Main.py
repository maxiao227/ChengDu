# -*- coding: utf-8 -*-
import time

import schedule
import yaml

if __name__ == '__main__':
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']


    # temp = DateClean(db_info)
    # # temp.getinit()
    # temp.clean()
    def job():
        print("I'm working...")


    schedule.every().minutes.do(job)
    #
    # congestionrank = CongestionRank(db_info)
    # # schedule.every().days.do(congestionrank.deal())
    while True:
        schedule.run_pending()
        time.sleep(1)
