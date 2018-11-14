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
        self.cursor = self.conn.cursor()

    @staticmethod
    def __getConn(db_info):
        # 静态方法，从连接池中取出连接
        if OraclePool.__pool is None:
            __pool = PooledDB(cx_Oracle, user=db_info['user'], password=db_info['password'], dsn=db_info['dsn'],
                              mincached=2,
                              maxcached=2, maxshared=2, maxconnections=2)
        return __pool.connection()

    def query(self, sql):
        self.cursor.execute(sql)
        return self.get_rows()

    # 提取数据，参数一提取的记录数，参数二，是否以字典方式提取。为true时返回：{'字段1':'值1','字段2':'值2'}
    def get_rows(self, size=None, is_dict=True):
        if size is None:
            rows = self.cursor.fetchall()
        else:
            rows = self.cursor.fetchmany(size)
        if rows is None:
            rows = []
        if is_dict:
            dict_rows = []
            dict_keys = [r[0].lower() for r in self.cursor.description]
            for row in rows:
                dict_rows.append(dict(zip(dict_keys, row)))
            rows = dict_rows
        return rows

    def insert(self, sql):
        self.cursor.execute(sql)

    def update(self, sql):
        self.cursor.execute(sql)

    # 提交
    def commit(self):
        self.conn.commit()

    # 回滚
    def rollback(self):
        self.conn.rollback();

    # 销毁
    def __del__(self):
        self.close()

    # 关闭连接
    def close(self):
        self.commit()
        self.cursor.close()
        self.conn.close()
