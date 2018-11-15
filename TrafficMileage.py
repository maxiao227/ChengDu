# -*- coding: utf-8 -*-
import datetime
import time

from Oracle import Oracle


class TrafficMileage:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
        beforTime = (datetime.datetime.now() - datetime.timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:00")
        perioTime = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:00")
        week1 = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime("%Y-%m-%d %H:%M:00")
        week2 = (datetime.datetime.now() - datetime.timedelta(days=21)).strftime("%Y-%m-%d %H:%M:00")
        week3 = (datetime.datetime.now() - datetime.timedelta(days=28)).strftime("%Y-%m-%d %H:%M:00")
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sql = "SELECT FBD ," \
              " SUM(CASE  WHEN STATUS = 3 THEN 1 ELSE 0 END) AS jam," \
              " SUM(CASE  WHEN STATUS = 0 THEN 1 ELSE 0 END) AS unknown," \
              " SUM(CASE  WHEN STATUS = 1 THEN 1 ELSE 0 END) AS unblocked ," \
              " SUM(CASE  WHEN STATUS = 2 THEN 1 ELSE 0 END) AS slow," \
              " COUNT(FBD)  FROM TD_ROAD_STATUS WHERE" \
              " TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')" \
                                               " AND TIME > TO_DATE('" + beforTime + "', 'YYYY-MM-DD HH24:MI:SS') GROUP BY FBD"
        rows = self.ora.selectall(sql)
        overalLength = 0
        LengthofCongestion = 0
        for row in rows:
            fbd = row[0]
            length = self.extent(fbd)
            if length == 0:
                continue
            else:
                if row[1] > 0:
                    overalLength += length
                    LengthofCongestion += length
                else:
                    LengthofCongestion += length
        if LengthofCongestion == 0:
            ratio = 0
        else:
            ratio = round((overalLength / LengthofCongestion) * 100, 2)
        sqlPerio = "SELECT CONGESTION_MILEAGE FROM AV_CONGESTION_MILEAGE WHERE SAVE_TIME = TO_DATE('" + perioTime + "', 'YYYY-MM-DD HH24:MI:SS')"
        rowsPerio = self.ora.selectall(sqlPerio)
        if rowsPerio:
            rowPerio = rowsPerio[0]
            lengthPerio = rowPerio[0]
        else:
            lengthPerio = 0
        sqlHistory = "SELECT AVG(CONGESTION_MILEAGE) FROM AV_CONGESTION_MILEAGE " \
                     "WHERE (SAVE_TIME = TO_DATE('" + week1 + "', 'YYYY-MM-DD HH24:MI:SS') OR SAVE_TIME = TO_DATE('" + week2 + "', 'YYYY-MM-DD HH24:MI:SS') OR SAVE_TIME = TO_DATE('" + week3 + "', 'YYYY-MM-DD HH24:MI:SS'))"
        rowsHis = self.ora.selectall(sqlHistory)

        rowHis = rowsHis[0]
        lengthHis = rowHis[0]
        if lengthHis == None:
            lengthHi = 0

        sqlInsert = "INSERT INTO JHGX.AV_CONGESTION_MILEAGE (ID, CONGESTION_MILEAGE, MILEAGE_COUNT, RATIO, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, SAVE_TIME, BATCH_NO) VALUES " \
                    "(sys_guid(), " + str(overalLength) + ", " + str(LengthofCongestion) + ", '" + str(
            ratio) + "', '" + str(lengthHis) + "', '" + str(lengthPerio) + "', sysdate, '" + str(BATCH_NO) + "')"
        pass

    def extent(self, fbd):
        if fbd.startswith('FBD_YH'):
            return 500
        elif fbd.startswith('FBD_EH'):
            return 800
        elif fbd.startswith('FBD_SH'):
            return 1000
        else:
            return 0
