# -*- coding: utf-8 -*-

from Oracle import Oracle


class IndexofTheTraffic:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        for i in range(720):
            sql = "SELECT FBD,JINDEX,STATUS,TIME FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate, 'dd')+" + str(
                i) + "/720 AND FBD = 'FBD_EHN_30'"
            