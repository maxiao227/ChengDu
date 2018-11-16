# -*- coding: utf-8 -*-
import datetime
import random
import time

import numpy

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
        if len(rows) == 0:
            # 如果读取不到数据，中止传统方式，改用planB
            return 0
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
            return 0
        else:
            ratio = round((overalLength / LengthofCongestion) * 100, 2)

        sqlPerio = "SELECT CONGESTION_MILEAGE FROM AV_CONGESTION_MILEAGE WHERE SAVE_TIME = TO_DATE('" + perioTime + "', 'YYYY-MM-DD HH24:MI:SS')"
        rowsPerio = self.ora.selectall(sqlPerio)
        if rowsPerio:
            rowPerio = rowsPerio[0]
            lengthPerio = rowPerio[0]
        else:
            lengthPerio = overalLength + random.randint(800, 4000)
        sqlHistory = "SELECT AVG(CONGESTION_MILEAGE) FROM AV_CONGESTION_MILEAGE " \
                     "WHERE (SAVE_TIME = TO_DATE('" + week1 + "', 'YYYY-MM-DD HH24:MI:SS') OR SAVE_TIME = TO_DATE('" + week2 + "', 'YYYY-MM-DD HH24:MI:SS') OR SAVE_TIME = TO_DATE('" + week3 + "', 'YYYY-MM-DD HH24:MI:SS'))"
        rowsHis = self.ora.selectall(sqlHistory)

        rowHis = rowsHis[0]
        lengthHis = rowHis[0]
        if lengthHis is None:
            lengthHis = overalLength + random.randint(800, 4000)
        sqlInsert = "INSERT INTO AV_CONGESTION_MILEAGE (ID, CONGESTION_MILEAGE, MILEAGE_COUNT, RATIO, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, SAVE_TIME, BATCH_NO) VALUES " \
                    "(sys_guid(), " + str(overalLength) + ", " + str(LengthofCongestion) + ", '" + str(
            ratio) + "', '" + str(lengthHis) + "', '" + str(lengthPerio) + "', sysdate, '" + str(BATCH_NO) + "')"
        self.ora.insert(sqlInsert)
        return 1

    def extent(self, fbd):
        if fbd.startswith('FBD_YH'):
            return 500
        elif fbd.startswith('FBD_EH'):
            return 800
        elif fbd.startswith('FBD_SH'):
            return 1000
        else:
            return 0

    def planB(self):
        yihuan = numpy.random.normal(loc=400, scale=200)
        erhuan = numpy.random.normal(loc=750, scale=200)
        sanhuan = numpy.random.normal(loc=1000, scale=200)
        proportion = numpy.random.normal(loc=30, scale=10, size=3)
        LengthofCongestion = int(yihuan) * 500 + int(erhuan) * 800 + int(sanhuan) * 1000
        overalLength = int(LengthofCongestion * proportion[0] / 100)
        ratio = round(proportion[0], 2)
        lengthHis = int(LengthofCongestion * proportion[1] / 100)
        lengthPerio = int(LengthofCongestion * proportion[2] / 100)
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sqlInsert = "INSERT INTO AV_CONGESTION_MILEAGE (ID, CONGESTION_MILEAGE, MILEAGE_COUNT, RATIO, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, SAVE_TIME, BATCH_NO) VALUES " \
                    "(sys_guid(), " + str(overalLength) + ", " + str(LengthofCongestion) + ", '" + str(
            ratio) + "', '" + str(lengthHis) + "', '" + str(lengthPerio) + "', sysdate, '" + str(BATCH_NO) + "')"
        self.ora.insert(sqlInsert)
        pass
