# -*- coding: utf-8 -*-
from Oracle import Oracle


class CongestionRank:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        sql = "SELECT FBD, AVG(JINDEX) AS rank FROM TD_ROAD_STATUS WHERE TIME < trunc(sysdate - 10) AND TIME > trunc(sysdate - 1 - 100) GROUP BY FBD ORDER BY rank DESC"
        rows = self.clean(self.ora.selectall(sql))

        for i in range(len(rows)):
            row = rows[i]
            ranking = i + 1
            fbd = row[0]
            jindex = row[1]
            sqlInsert = "INSERT INTO JHGX.AV_CONGESTION_RANKING (ID, FBD, RANK, SAVE_TIME, JINDEX) VALUES (sys_guid(), '" + fbd + "', " + str(
                ranking) + ", sysdate, " + str(
                jindex)[0:4] + ")"
            self.ora.insert(sqlInsert)

    def clean(self, rows):
        result = []
        for row in rows:
            if row[1] is not None:
                result.append(row)
        return result
