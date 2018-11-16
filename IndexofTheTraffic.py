# -*- coding: utf-8 -*-
import datetime
import random
import time

from Oracle import Oracle


class IndexofTheTraffic:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
        BATCH_NO = time.strftime("%G%m%d%H%M")
        sql = "SELECT AVG(JINDEX), AVG(STATUS), TIME FROM TD_ROAD_STATUS WHERE (" \
              "TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS') AND time > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS') - 1 / 720 AND FBD LIKE 'FBD_EH%') GROUP BY TIME"
        rows = self.ora.selectall(sql)
        if len(rows) > 0:
            row = rows[0]
            traffic_index = row[0]
            traffic_status = row[1]
        else:
            return False
        sqlLastWeek = "SELECT AVG(JINDEX), TIME FROM TD_ROAD_STATUS WHERE (" \
                      "TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-7 AND " \
                                                      "TIME > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-7-1/720 AND FBD LIKE 'FBD_EH%') GROUP BY TIME"
        rowsLastWeek = self.ora.selectall(sqlLastWeek)
        if len(rowsLastWeek) > 0:
            rowLastWeek = rowsLastWeek[0]
            same_period_last_week_index = rowLastWeek[0]
        else:
            return False
        sqlHistory = "SELECT AVG(JINDEX), to_char(TIME, 'hh24:mi:ss') FROM TD_ROAD_STATUS WHERE ((" \
                     "TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-7 AND time > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-7 - 1 / 720) OR (TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-14 AND TIME > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-14 - 1 / 720) OR (TIME <= TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-21 AND TIME > TO_DATE('" + nowTime + "', 'YYYY-MM-DD HH24:MI:SS')-21 - 1 / 720)) AND FBD LIKE 'FBD_EH%' GROUP BY to_char(TIME, 'hh24:mi:ss')"
        rowsHistory = self.ora.selectall(sqlHistory)
        if len(rowsHistory) > 0:
            rowHistory = rowsHistory[0]
            historical_average_index = rowHistory[0]
        else:
            return False
        INDEX_INC_DEC = traffic_index - same_period_last_week_index
        sqlInsert = "INSERT INTO AV_TRAFFIC_INDEX (ID, TRAFFIC_INDEX, TRAFFIC_STATUS, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, INDEX_INC_DEC, SAVE_TIME, BATCH_NO, SEGMENT) VALUES (sys_guid(),'" + str(
            traffic_index) + "', '" + str(traffic_status) + "', '" + str(historical_average_index) + "','" + str(
            same_period_last_week_index) + "', '" + str(INDEX_INC_DEC) + "', sysdate, '" + BATCH_NO + "', '二环')"
        self.ora.insert(sqlInsert)
        return True

    def planB(self):
        BATCH_NO = time.strftime("%G%m%d%H%M")
        traffic_status = random.randint(1, 3)
        traffic_index = round(random.uniform(1, 10), 2)
        historical_average_index = round(random.uniform(1, 10), 2)
        same_period_last_week_index = round(random.uniform(1, 10), 2)
        INDEX_INC_DEC = round((traffic_index - same_period_last_week_index), 2)
        sqlInsert = "INSERT INTO AV_TRAFFIC_INDEX (ID, TRAFFIC_INDEX, TRAFFIC_STATUS, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, INDEX_INC_DEC, SAVE_TIME, BATCH_NO, SEGMENT) VALUES (sys_guid(),'" + str(
            traffic_index) + "', '" + str(traffic_status) + "', '" + str(historical_average_index) + "','" + str(
            same_period_last_week_index) + "', '" + str(INDEX_INC_DEC) + "', sysdate, '" + BATCH_NO + "', '二环')"
        self.ora.insert(sqlInsert)
        pass
