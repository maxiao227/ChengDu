# coding=utf-8
'''
oracle 数据库操作封装 by zhengyangbo 20160329
'''
import cx_Oracle
from DBUtils.PooledDB import PooledDB


class OraclePool():
    __pool = None  # 连接池对象

    def __init__(self, db_info=None):
        # 连接池方式
        self.db_info = db_info
        self.conn = OraclePool.__getConn(db_info)

    @staticmethod
    def __getConn(db_info):
        # 静态方法，从连接池中取出连接
        if OraclePool.__pool is None:
            __pool = PooledDB(cx_Oracle, user=db_info['user'], password=db_info['password'], dsn=db_info['dsn'],
                              mincached=2,
                              maxcached=2, maxshared=2, maxconnections=2)
        return __pool.connection()

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
