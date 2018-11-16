# -*- coding: utf-8 -*-
import datetime
import time

from Oracle import Oracle


class CongestionRank:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        # self.ora = OraclePool(db_info)
        pass

    def deal(self):
        # nowTime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:00")
        nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sql = "SELECT FBD, AVG(JINDEX) AS rank FROM TD_ROAD_STATUS WHERE TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS') AND TIME > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-1/288 GROUP BY FBD ORDER BY rank DESC"
        rows = self.clean(self.ora.selectall(sql))
        if len(rows) == 0:
            return False
        else:
            for i in range(len(rows)):
                row = rows[i]
                ranking = i + 1
                fbd = row[0]
                jindex = round(row[1], 2)
                sqlInsert = "INSERT INTO JHGX.AV_CONGESTION_RANKING (ID, FBD, RANK, JINDEX, SAVE_TIME, BATCH_NO) VALUES (sys_guid(), '" + str(
                    fbd) + "', " + str(ranking) + " ,'" + str(jindex) + "',sysdate, '" + BATCH_NO + "')"
                self.ora.insert(sqlInsert)
        return True

    def clean(self, rows):
        result = []
        for row in rows:
            if row[1] is not None:
                result.append(row)
        return result

    def planB(self):
        nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sql = "SELECT FBD, AVG(JINDEX) AS rank FROM TD_ROAD_STATUS WHERE TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS') AND TIME > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-360 GROUP BY FBD ORDER BY rank DESC"
        rows = self.clean(self.ora.selectall(sql))
        if len(rows) == 0:
            return
        else:
            for i in range(len(rows)):
                row = rows[i]
                ranking = i + 1
                fbd = row[0]
                jindex = round(row[1], 2)
                sqlInsert = "INSERT INTO JHGX.AV_CONGESTION_RANKING (ID, FBD, RANK, JINDEX, SAVE_TIME, BATCH_NO) VALUES (sys_guid(), '" + str(
                    fbd) + "', " + str(ranking) + " ,'" + str(jindex) + "',sysdate, '" + BATCH_NO + "')"
                self.ora.insert(sqlInsert)
