# -*- coding: utf-8 -*-
class IndexofTheTraffic:
    def __init__(self, ora):
        self.ora = ora
        pass

    def getinit(self):
        # for n in range(720):
        #     sql = "SELECT FBD, STATUS, TIME FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate, 'dd') + " + str(
        #         n) + " / 720 AND FBD = 'FBD_EHN_30' ORDER BY TIME"
        #     rows = self.ora.query(sql)
        #     if len(rows) == 0:
        #         sql = "INSERT INTO JHGX.TD_ROAD_STATUS (ID, FBD, JINDEX, SPEED, VOL, OCC, STATUS, TIME) VALUES (sys_guid(), 'FBD_EHN_30', 4.64, 44.54, 41.00, 9.95, '1', trunc(sysdate, 'dd') + " + str(
        #             n) + " / 720)"
        #         self.ora.insert(sql)

        sql = "SELECT * FROM TD_ROAD_STATUS WHERE TIME > trunc(sysdate, 'dd') AND FBD = 'FBD_EHN_30' ORDER BY TIME"
        rows = self.ora.query(sql)
        for row in rows:
            temp = row['id']
            time = row['time']
            print(type(str(time)))
            if temp == '' or temp is None:
                sql = "UPDATE TD_ROAD_STATUS SET ID = sys_guid() WHERE TIME = TO_DATE('" + str(
                    time) + "', 'YYYY-MM-DD HH24:MI:SS')"
                self.ora.update(sql)
