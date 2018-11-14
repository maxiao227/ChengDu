# -*- coding: utf-8 -*-
import yaml

from IndexofTheTraffic import IndexofTheTraffic
from Oracle import Oracle

if __name__ == '__main__':
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']
    ora = Oracle(db_info=db_info)
    temp = IndexofTheTraffic(ora)
    temp.getinit()
