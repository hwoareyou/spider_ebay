# -*- coding: utf-8 -*-
import pymysql

class MysqlDb():

    def __init__(self):
        conn = pymysql.connect(
            host="47.244.242.187",
            port=3306,
            user="lilei",
            passwd="BLkj_123123",
            db="amazondb",
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        # conn = pymysql.connect(
        #     host="localhost",
        #     port=7788,
        #     user="root",
        #     passwd="123123",
        #     db="amazondb",
        #     charset='utf8mb4',
        #     cursorclass=pymysql.cursors.DictCursor
        # )
        # conn = pymysql.connect(
        #     host="172.16.3.59",
        #     port=3306,
        #     user="root",
        #     passwd="123456",
        #     db="amazondb",
        #     charset='utf8mb4',
        #     cursorclass=pymysql.cursors.DictCursor
        # )
        self.mysql = conn

    def insert(self,sql,value):
        cur = self.mysql.cursor()
        cur.executemany(sql,value)
        cur.execute('commit')


    def select(self,sql):
        cur = self.mysql.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        return res


    def update(self,sql,value):
        cur = self.mysql.cursor()
        cur.executemany(sql,value)
        cur.execute('commit')


    def close(self):
        self.mysql.close()
