# -*- coding: utf-8 -*-
import yaml

from DateClean import DateClean

if __name__ == '__main__':
    with open('db.yaml', 'r') as f:
        db_info = yaml.load(f.read())
        user = db_info['user']
        password = db_info['password']
        dsn = db_info['dsn']
    temp = DateClean(db_info)
    # temp.getinit()
    temp.clean()
