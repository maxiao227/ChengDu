# -*- coding: utf-8 -*-
import time

from Oracle import Oracle


class CongestionRank:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sql = "SELECT FBD, AVG(JINDEX) AS rank FROM TD_ROAD_STATUS WHERE TIME < trunc(sysdate - 10) AND TIME > trunc(sysdate - 1 - 100) GROUP BY FBD ORDER BY rank DESC"
        rows = self.clean(self.ora.selectall(sql))
        for i in range(len(rows)):
            row = rows[i]
            ranking = i + 1
            fbd = row[0]
            jindex = row[1]
            sqlInsert = "INSERT INTO JHGX.AV_CONGESTION_RANKING (ID, FBD, RANK, JINDEX, SAVE_TIME, BATCH_NO) VALUES (sys_guid(), '" + str(
                fbd) + "', " + str(ranking) + " ,'" + str(jindex)[0:4] + "',sysdate, '" + BATCH_NO + "')"
            self.ora.insert(sqlInsert)

    def clean(self, rows):
        result = []
        for row in rows:
            if row[1] is not None:
                result.append(row)
        return result
