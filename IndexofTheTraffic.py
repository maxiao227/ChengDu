# -*- coding: utf-8 -*-
import time

from Oracle import Oracle


class IndexofTheTraffic:
    def __init__(self, db_info):
        self.ora = Oracle(db_info)
        pass

    def deal(self):
        BATCH_NO = time.strftime("%G%m%d%H%M")
        for i in range(720):
            sql = "SELECT AVG(JINDEX), AVG(STATUS), TIME FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate - 1) + " + str(
                i) + " / 720 AND FBD LIKE 'FBD_EH%' GROUP BY TIME"
            rows = self.ora.selectall(sql)
            row = rows[0]
            traffic_index = row[0]
            traffic_status = row[1]
            sqlLastWeek = "SELECT AVG(JINDEX), TIME FROM TD_ROAD_STATUS WHERE TIME = trunc(sysdate - 7 - 1) + " + str(
                i) + " / 720 AND FBD LIKE 'FBD_EH%' GROUP BY TIME"
            rowsLastWeek = self.ora.selectall(sqlLastWeek)
            rowLastWeek = rowsLastWeek[0]
            same_period_last_week_index = rowLastWeek[0]
            sqlHistory = "SELECT AVG(JINDEX), to_char(TIME, 'hh24:mi:ss') FROM TD_ROAD_STATUS WHERE " \
                         "(TIME = trunc(sysdate - 7) + " + str(
                i) + " / 720 OR TIME = trunc(sysdate - 14) + " + str(
                i) + " / 720 OR TIME = trunc(sysdate - 21) + " + str(
                i) + " / 720 OR TIME = trunc(sysdate - 28) + " + str(
                i) + " / 720) AND FBD LIKE 'FBD_EH%' GROUP BY to_char(TIME, 'hh24:mi:ss')"
            rowsHistory = self.ora.selectall(sqlHistory)
            rowHistory = rowsHistory[0]
            historical_average_index = rowHistory[0]
            INDEX_INC_DEC = traffic_index - same_period_last_week_index
            sqlInsert = "INSERT INTO AV_TRAFFIC_INDEX (ID, TRAFFIC_INDEX, TRAFFIC_STATUS, HISTORICAL_AVERAGE_INDEX, SAME_PERIOD_LAST_WEEK_INDEX, INDEX_INC_DEC, SAVE_TIME, BATCH_NO, SEGMENT) VALUES (sys_guid(),'" + str(
                traffic_index) + "', '" + str(traffic_status) + "', '" + str(historical_average_index) + "','" + str(
                same_period_last_week_index) + "', '" + str(INDEX_INC_DEC) + "', sysdate, '" + BATCH_NO + "', '二环')"
            self.ora.insert(sqlInsert)
