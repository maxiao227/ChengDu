# -*- coding: utf-8 -*-
import cx_Oracle


class Oracle:
    __pool = None  # 连接池对象

    def __init__(self, db_info):
        # 连接池方式
        self.db_info = db_info
        self.conn = cx_Oracle.connect(user=db_info['user'], password=db_info['password'], dsn=db_info['dsn'])

    def insert(self, sql):
        try:
            print(sql)
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            self.conn.commit()
        except Exception as e:
            self.conn.close()
            print(e)

    def selectall(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            print(sql)
            return result
        except Exception as e:
            self.conn.close()
            print(e)

    def update(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            print(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.close()
            print(e)

    def delete(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            print(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.close()
            print(e)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def sqltran(self, sql, param):
        for i in range(0, len(param)):
            sql = sql.replace('%s', str(param[i]), 1)
        return sql
