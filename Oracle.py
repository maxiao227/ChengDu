# -*- coding: utf-8 -*-
import cx_Oracle


class Oracle:
    __pool = None  # 连接池对象

    def __init__(self, db_info):
        # 连接池方式
        self.db_info = db_info
        self.conn = cx_Oracle.connect(user=db_info['user'], password=db_info['password'], dsn=db_info['dsn'])
        self.cursor = self.conn.cursor()

    def insert(self, sql):
        try:
            print(sql)
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.close()
            print(e)

    def selectall(self, sql):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            print(sql)
            return result
        except Exception as e:
            self.conn.close()
            print(e)

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            print(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.close()
            print(e)

    def delete(self, sql):
        try:
            self.cursor.execute(sql)
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
