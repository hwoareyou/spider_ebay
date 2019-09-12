# -*- coding: utf-8 -*-
# @Time    : 2018/12/18 11:06
# @Author  : Liu
# @File    : mylog.py
# @Software: PyCharm

import os,logging

class Mylog():

    def __init__(self, logname):
        self.logname = logname

    def logs(self):
        logpath = os.getcwd() + "/log/"
        if not os.path.exists(logpath):
            os.makedirs(logpath)
        log = logging.getLogger("spider_logger")
        log.setLevel(logging.INFO)
        # 创建一个日志处理器
        ## 这里需要正确填写路径和文件名，拼成一个字符串，最终生成一个log文件
        logHandler = logging.FileHandler(filename=logpath + self.logname + ".log")
        ## 设置日志级别
        logHandler.setLevel(logging.INFO)
        # 创建一个日志格式器
        formats = logging.Formatter('%(asctime)s %(levelname)s: %(message)s',
                                    datefmt='[%Y-%m-%d %H:%M:%S]')

        # 将日志格式器添加到日志处理器中
        logHandler.setFormatter(formats)
        # 将日志处理器添加到日志记录器中
        log.addHandler(logHandler)
        return log
