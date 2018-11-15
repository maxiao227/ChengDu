# -*- coding: utf-8 -*-

import yaml

from IndexofTheTraffic import IndexofTheTraffic

if __name__ == '__main__':
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']


    def indexofthetrafficcontrolControl():
        indexofthetraffic = IndexofTheTraffic(db_info)
        indexofthetraffic.deal()


    indexofthetrafficcontrolControl()

    # temp = DateClean(db_info)
    # # temp.getinit()
    # temp.clean()
    # congestionrank = CongestionRank(db_info)
    # # schedule.every().days.do(congestionrank.deal())

    # schedule.every().days.at('10:30').do(indexofthetrafficcontrolControl)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
