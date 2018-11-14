# -*- coding: utf-8 -*-
import cx_Oracle


class Oracle():
    __pool = None  # 连接池对象

    def __init__(self, db_info=None):
        # 连接池方式
        self.db_info = db_info
        self.conn = cx_Oracle.connect(cx_Oracle, user=db_info['user'], password=db_info['password'], dsn=db_info['dsn'])
        self.cursor = self.conn.cursor()

    def insert(self, sql, param, flag=True):
        sql = self.sqltran(sql, param)
        self.cursor.execute(sql)
        self.conn.commit()

    def selectall(self, sql, param=None):
        sql = self.sqltran(sql, param)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def update(self, sql, param=None, flag=True):
        sql = self.sqltran(sql, param)
        self.cursor.execute(sql)
        self.conn.commit()

    def delete(self, sql, param=None):
        sql = self.sqltran(sql, param)
        self.cursor.execute(sql)
        self.conn.commit()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def sqltran(self, sql, param):
        for i in range(0, len(param)):
            sql = sql.replace('%s', str(param[i]), 1)
        return sql

    # if __name__ == '__main__':
    #     param = ('id', 'name', 'addr', 1)
    #     sql1 = 'select id ,name, addr from test'
    #     sql0 = 'select %s,%s,%s from test'
    #     sql2 = 'insert into test (%s,%s,%s)values(4,4,4)'
    #     sql3 = 'update test set name=222 where id=%s'
    #     sql4 = 'delete from test where id=%s'
    #
    #     print(oracle.selectall(sql0, param))
    #     print(oracle.insert(sql2, param))
    #     print(oracle.selectOne(sql0, param))
    #     print(oracle.update(sql3, ('3')))
    #     print(oracle.delete(sql4, ('4')))
    #     print(oracle.selectall(sql0, param))
    #     print('success')
