# -*- coding: utf-8 -*-
import datetime
import random

from Oracle import Oracle


class DateClean:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def getinit(self):
        sql = "SELECT * FROM TD_ROAD_STATUS WHERE TIME > trunc(sysdate, 'dd') AND FBD = 'FBD_EHN_30' ORDER BY TIME"
        rows = self.ora.selectall(sql)
        for row in rows:
            temp = row[2]
            time = row[7]
            rd = random.uniform(1, 20)
            if temp == '' or temp is None:
                sql = "UPDATE TD_ROAD_STATUS SET JINDEX = " + str(round(rd, 2)) + " WHERE TIME = TO_DATE('" + str(
                    time) + "', 'YYYY-MM-DD HH24:MI:SS')"
                self.ora.update(sql)

    def clean(self):
        for i in range(720 - 197):
            i = i + 197
            sql = "SELECT * FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate, 'dd')+" + str(
                i) + "/720 AND FBD='FBD_EHN_30'"
            rows = self.ora.selectall(sql)
            if len(rows) > 1:
                for j in range(len(rows) - 1):
                    sqlDel = "DELETE FROM TD_ROAD_STATUS WHERE id = '" + rows[j][0] + "'"
                    self.ora.delete(sqlDel)

    def copy(self):
        for i in range(720 - 198):
            i = i + 198
            sql = "SELECT * FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate, 'dd')+" + str(
                i) + "/720 AND FBD='FBD_EHN_30'"
            rows = self.ora.selectall(sql)
            row = rows[0]
            for j in range(30):
                time = row[7] - datetime.timedelta(days=j)
                sqlINSERT = "INSERT INTO JHGX.TD_ROAD_STATUS (ID, FBD, JINDEX, SPEED, VOL, OCC, STATUS, TIME) VALUES (sys_guid(), 'FBD_EHNS_02', " + str(
                    row[2])[0:5] + ", " + self.changeNull(str(row[3])) + ", " + self.changeNull(
                    str(row[4])) + ", " + self.changeNull(str(row[5])) + ", '" + str(
                    row[6]) + "', TO_DATE('" + str(time) + "', 'YYYY-MM-DD HH24:MI:SS'))"
                self.ora.insert(sqlINSERT)

    def changeNull(self, aaa):
        if aaa == 'None':
            return 'null'
        else:
            return aaa

    def clean2(self):
        for i in range(21600):
            sql = "SELECT * FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate, 'dd')-" + str(
                i) + "/720 AND FBD='FBD_EHN_30'"
            rows = self.ora.selectall(sql)
            if len(rows) > 1:
                for j in range(len(rows) - 1):
                    sqlDel = "DELETE FROM TD_ROAD_STATUS WHERE id = '" + rows[j][0] + "'"
                    self.ora.delete(sqlDel)
