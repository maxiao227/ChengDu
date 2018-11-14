# -*- coding: utf-8 -*-
import cx_Oracle
import yaml
from DBUtils.PooledDB import PooledDB

with open('db.yaml', 'r') as f:
    temp = yaml.load(f.read())
    user = temp['user']
    password = temp['password']
    dsn = temp['dsn']
pool = PooledDB(cx_Oracle, user=user, password=password, dsn=dsn, mincached=2,
                maxcached=2, maxshared=2, maxconnections=2)
# pool = PooledDB(cx_Oracle, user="jhgx", password="jhgx", dsn="192.168.0.108:1521/hiatmp", mincached=2,
#                 maxcached=2, maxshared=2, maxconnections=2)
conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
cur = conn.cursor()

SQL = "select * from dual"
r = cur.execute(SQL)
r = cur.fetchall()
print(r)
cur.close()
conn.close()
