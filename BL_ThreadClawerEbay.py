# -*- coding: utf-8 -*-
# @Author   : liu
# 加入日志
from selenium.webdriver import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
import time
import json,re,os,urllib.request,datetime,random,requests,sys,socket
from lxml import etree
from mysql_utils.mysql_db import MysqlDb
from baidu_OCR import recognition_character
import threading
from threading import Thread
from queue import Queue
from tengxun_OCR import Ocr
from selenium.webdriver.chrome.options import Options
from log_utils.mylog import Mylog
from PIL import Image
import traceback
import warnings
warnings.filterwarnings('ignore')

class ThreadClawerWish(Thread):

    def __init__(self, i, product_link_queue, product_info_queue, user_id):
        '''
        :param i: 线程编号
        :param product_link_queue:商品链接队列
        :param product_info_queue: 商品信息队列
        :param user_id: 用户id
        '''
        Thread.__init__(self)
        self.user_id = user_id
        self.mysql = MysqlDb()
        self.threadName = '采集线程' + str(i)

        self.product_link_queue = product_link_queue
        self.product_info_queue = product_info_queue

        pass

    # 解析提取商品数据
    def __parseProduct__(self, html, product_link):
        try:

            product_html = etree.HTML(html)
            product_info = {}

            # 商品ID
            product_id = re.search(r'/(\d+)\?',product_link).group(1)
            product_info['product_id'] = product_id
            # 商品名称
            product_name = str(product_html.xpath('//*[@id="itemTitle"]/text()')[0])
            product_info['product_name'] = product_name
            # 商品链接
            product_info['product_url'] = product_link
            # 商品价格
            price = product_html.xpath('//*[@id="prcIsum"]/text()')
            if price:
                price = str(price[0])
            else:
                price = str(product_html.xpath('//*[@id="mm-saleDscPrc"]/text()')[0])
            product_info['price'] = price
            # 商品卖家
            seller_name = str(product_html.xpath('//div[@class="mbg vi-VR-margBtm3"]/a/span/text()')[0])
            product_info['seller_name'] = seller_name
            # 描述
            description_url = str(product_html.xpath('//iframe[@id="desc_ifr"]/@src')[0])
            description = self.__get_description__(description_url,product_link)
            product_info['description'] = description

            description_img = re.findall(r'src="(.+?)"',description)
            product_info['description_img'] = [item for item in description_img if not item.endswith('.js')]

            # 规格属性
            specifics_ele_list = product_html.xpath('//*[@id="viTabs_0_is"]//tr')
            specifics = {}
            for item in specifics_ele_list:
                td_ele_list = item.xpath('./td')
                if len(td_ele_list) > 1:
                    for i in range(0,len(td_ele_list),2):
                        k = td_ele_list[i].text.strip().replace(':','')
                        v = str(td_ele_list[i+1].xpath('string(.)')).strip().replace('\n','').replace('\t','').replace('Read moreabout the condition','')
                        specifics[k] = v
                elif len(td_ele_list) > 0:

                    k = item.xpath('./th')[0].text
                    v = str(item.xpath('string(./td)')).strip().replace('\n','').replace('\t','').replace('Read moreabout the condition','').replace('    ','')

                    specifics[k] = v

            product_info['specifics'] = json.dumps(specifics)

            try:
                brand_name = specifics['Brand'].upper()
            except:
                brand_name = ''

            product_info['brand_name'] = brand_name

            # 解析提取数据
            res_sub = re.search(r'rwidgets\((.+?);new \(raptor', html).group(1)
            # res_sub = res_sub.encode('utf-8').decode("unicode-escape").replace('\'', '"').replace('en"s', 'en\'s')
            res_sub = res_sub.encode('utf-8').decode("unicode-escape").replace('\'', '"')

            # 替换字符串中的 " 字符，避免json转义的时候出错（但要保留键值对中的 " 字符）
            def str_sub(s):
                r_index = s.find('"')
                l_index = s.rfind('"')
                l = list(s)
                l[r_index] = '*/*'
                l[l_index] = '*/*'
                s = ''.join(l).replace('"','').replace('*/*','"')
                return s

            res_sub = re.sub(r'"(.+?)"[:,}]', lambda x: str_sub(x.group(0)), res_sub)


            # 图片
            fsImgList = json.loads(re.search(r'fsImgList":(.+?),"isNavigationArrowsEnabled', res_sub).group(1))
            img_url_list = []
            for item in fsImgList:
                img_url = item['maxImageUrl']
                if not img_url:
                    img_url = item['displayImgUrl']
                if not img_url:
                    img_url = item['thumbImgUrl']
                if img_url:
                    img_url_list.append(img_url)
            product_info['img_url_list'] = img_url_list
            # 图片map
            menuItemPictureIndexMap = re.search(r'menuItemPictureIndexMap":(.+?),"itemVariationsMap', res_sub)
            if menuItemPictureIndexMap:
                if menuItemPictureIndexMap.group(1).upper() != 'NULL':
                    menuItemPictureIndexMap = json.loads(menuItemPictureIndexMap.group(1))
                else:
                    menuItemPictureIndexMap = {}
            else:
                menuItemPictureIndexMap = {}

            # 属性（Size、Color）
            menuItemMap = re.search(r'"menuItemMap":(.+),"menuItemPictureIndexMap', res_sub)

            if menuItemMap:
                menuItemMap = json.loads(menuItemMap.group(1))
            else:
                menuItemMap = {}
            map_img_list = []
            for k,v in menuItemMap.items():
                attr_ids = str(v['matchingVariationIds'])
                if k in list(menuItemPictureIndexMap.keys()):
                    try:
                        img_url = fsImgList[menuItemPictureIndexMap[k][0]]['maxImageUrl']
                        if not img_url:
                            img_url = fsImgList[menuItemPictureIndexMap[k][0]]['displayImgUrl']
                        if not img_url:
                            img_url = fsImgList[menuItemPictureIndexMap[k][0]]['thumbImgUrl']
                        if img_url:
                            map_img_list.append({'attr_ids':attr_ids,'img_url':img_url})
                    except:
                        pass

            product_info['map_img_list'] = map_img_list

            # # 属性map
            # menuModels = json.loads(re.search(r'"menuModels":(.+?),"menuItemMap', res_sub).group(1))
            # attr_list = []
            # for model in menuModels:
            #     attr_name = model['name']
            #     attr_value_list = [menuItemMap[str(value_id)]['valueName'] for value_id in model['menuItemValueIds']]
            #     attr_list.append({'attr_name': attr_name, 'attr_value': attr_value_list})
            #
            # product_info['attr_list'] = attr_list


            # 变体信息
            itemVariationsMap = re.search(r'"itemVariationsMap":(.+),"unavailableVariationIds', res_sub)
            if itemVariationsMap:
                itemVariationsMap = json.loads(itemVariationsMap.group(1))
            else:
                itemVariationsMap = {}
            attr_data_list = []
            for attr_id,attr_data in itemVariationsMap.items():
                quantityAvailable = attr_data['quantityAvailable']
                quantitySold = attr_data['quantitySold']
                price = attr_data['price']
                traitValuesMap = {k:menuItemMap[str(v)]['valueName'] for k,v in  attr_data['traitValuesMap'].items()}
                attr_data_list.append({'attr_id':attr_id,'traitValuesMap':traitValuesMap,'price':price,'quantityAvailable':quantityAvailable,'quantitySold':quantitySold})

            product_info['attr_data_list'] = attr_data_list


            print('商品信息：', product_info['product_id'], product_info['product_name'])

            return product_info

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


    def __get_description__(self,description_url,product_link):
        try:
            headers = {
                "Host": "vi.vipr.ebaydesc.com",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
                "User-Agent": get_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Referer": product_link,
                "Accept-Language": "zh-CN,zh;q=0.9",
                # "Cookie": "__gads=ID=4800ae0f6808deac:T=1566978230:S=ALNI_MaTlGIi35S1nvduTEx0cLBEu5JKlA; cid=AeV4IHdPND6uL3US%231655155015; ak_bmsc=4DCC408129C23C15D097EE4D6ED76FBA17D2D7BC5C49000030C76C5D5C7F2937~pl011yvcKmF/e6OPmIFJfH96Ajw70tFV7gahIRFNvBbANKHEmTP6xYBuvsyiD/0ntMR1dGUCfpifglhCFmhhPBqOBSUOIgVAZk40YtINBwNiroC/RBQgLx0BPHbctnTntE0zovMQKbN/SU0EcNrXEivy6K0jBYCY2cRc87CUBjX/U+ucJ1Gf7WsWRVapEAfqWutLoZ3rF5TH/4CkwcQYAEZsnyszI7Sar50l+21JoRBt0=; JSESSIONID=AA1E88CA3C8FA157AF7F6E1AA29B0D16; bm_sv=3C487E46949060362FC46EC129CD394A~D48vM8V/23G08UsD/OKzhkfFqUdQhdArTt+biadk+e/DzuoXHmBmAsONdf3SjKX/0xsMyjjIiBgy9bEFoUp7O+TGkIQli7JLwIEV81XFa5KJzy+vnQRnme1y53YA10hc6jONCCf89MHkfhUC8R5yqbAdnsHmGjJpwfeQYlMZXaI=; npii=btguid/d729537216c0a9cba0175b41fffb6bee612f346a^cguid/d7295e4416c0a4d129329faae979544c612f346a^; ns1=BAQAAAWzqHwjqAAaAANgATF9OAQ5jNzJ8NjAxXjE1NjczOTA3MTkzNTReXjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1dZDIMoMuws09DpPFbCOQUOYuFQw*; dp1=btzo/-1e05d6cdb9e^u1p/QEBfX0BAX19AQA**5f4e010e^bl/CN612f348e^pbf/%23e000e000008100020000005f4e010e^; s=CgAD4ACBdbh8OZDcyOTUzNzIxNmMwYTljYmEwMTc1YjQxZmZmYjZiZWUA7gBFXW4fDjE0Bmh0dHBzOi8vd3d3LmViYXkuY29tL3N0ci9CVVktQ09PTC1TVFVGRi0xMD9fdHJrc2lkPXAyMDQ3Njc1LmwyNTYzByCvnAA*; nonsession=BAQAAAWzqHwjqAAaAAAgAHF2UWo4xNTY3NDExNTEzeDE4MjkzODgxMDkxM3gweDJOADMABl9OAQ41MTgwMDAAywACXWzUljI1AMoAIGbSzw5kNzI5NTM3MjE2YzBhOWNiYTAxNzViNDFmZmZiNmJlZdPnJ70Ap5vRZdIlA5f5pZHUefrw; ds2=sotr/b9YGZz13l27G^; ebay=%5Edv%3D5d6caac8%5Esbf%3D%2310000000100%5Ejs%3D1%5Epsi%3DA6oLb2o8*%5E"
            }
            try:
                res = requests.get(description_url, headers=headers, verify=False, timeout=30)
            except:
                count = 1
                while count <= 5:
                    try:
                        res = requests.get(description_url, headers=headers, verify=False, timeout=30)
                        break
                    except:
                        err_info = '__get_description__ reloading for %d time' % count if count == 1 else '__get_description__ reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__get_description__ job failed!")
                    return

            return res.text

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 通过requests请求数据
    def __request__(self, product_link):
        try:

            headers = {
                "Host": "www.ebay.com",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
                "User-Agent": get_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": product_link,
                "Accept-Language": "zh-CN,zh;q=0.9",
                # "Cookie": "__gads=ID=4800ae0f6808deac:T=1566978230:S=ALNI_MaTlGIi35S1nvduTEx0cLBEu5JKlA; cid=AeV4IHdPND6uL3US%231655155015; ak_bmsc=4DCC408129C23C15D097EE4D6ED76FBA17D2D7BC5C49000030C76C5D5C7F2937~pl011yvcKmF/e6OPmIFJfH96Ajw70tFV7gahIRFNvBbANKHEmTP6xYBuvsyiD/0ntMR1dGUCfpifglhCFmhhPBqOBSUOIgVAZk40YtINBwNiroC/RBQgLx0BPHbctnTntE0zovMQKbN/SU0EcNrXEivy6K0jBYCY2cRc87CUBjX/U+ucJ1Gf7WsWRVapEAfqWutLoZ3rF5TH/4CkwcQYAEZsnyszI7Sar50l+21JoRBt0=; JSESSIONID=AA1E88CA3C8FA157AF7F6E1AA29B0D16; bm_sv=3C487E46949060362FC46EC129CD394A~D48vM8V/23G08UsD/OKzhkfFqUdQhdArTt+biadk+e/DzuoXHmBmAsONdf3SjKX/0xsMyjjIiBgy9bEFoUp7O+TGkIQli7JLwIEV81XFa5KJzy+vnQRnme1y53YA10hc6jONCCf89MHkfhUC8R5yqbAdnsHmGjJpwfeQYlMZXaI=; npii=btguid/d729537216c0a9cba0175b41fffb6bee612f346a^cguid/d7295e4416c0a4d129329faae979544c612f346a^; ns1=BAQAAAWzqHwjqAAaAANgATF9OAQ5jNzJ8NjAxXjE1NjczOTA3MTkzNTReXjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1dZDIMoMuws09DpPFbCOQUOYuFQw*; dp1=btzo/-1e05d6cdb9e^u1p/QEBfX0BAX19AQA**5f4e010e^bl/CN612f348e^pbf/%23e000e000008100020000005f4e010e^; s=CgAD4ACBdbh8OZDcyOTUzNzIxNmMwYTljYmEwMTc1YjQxZmZmYjZiZWUA7gBFXW4fDjE0Bmh0dHBzOi8vd3d3LmViYXkuY29tL3N0ci9CVVktQ09PTC1TVFVGRi0xMD9fdHJrc2lkPXAyMDQ3Njc1LmwyNTYzByCvnAA*; nonsession=BAQAAAWzqHwjqAAaAAAgAHF2UWo4xNTY3NDExNTEzeDE4MjkzODgxMDkxM3gweDJOADMABl9OAQ41MTgwMDAAywACXWzUljI1AMoAIGbSzw5kNzI5NTM3MjE2YzBhOWNiYTAxNzViNDFmZmZiNmJlZdPnJ70Ap5vRZdIlA5f5pZHUefrw; ds2=sotr/b9YGZz13l27G^; ebay=%5Edv%3D5d6caac8%5Esbf%3D%2310000000100%5Ejs%3D1%5Epsi%3DA6oLb2o8*%5E"
            }


            try:
                res = requests.get(product_link, headers=headers, verify=False ,timeout=30)
            except:
                count = 1
                while count <= 5:
                    try:
                        res = requests.get(product_link, headers=headers, verify=False ,timeout=30)
                        break
                    except:
                        err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__request__ job failed!")
                    return

            return res.text

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 查询商品
    def __query_product__(self, product_id):

        sql = 'select id from amazonshop_goods  WHERE ASIN = \'%s\'  ' % product_id
        res1 = self.mysql.select(sql)
        sql = 'select id from amazonshop_deletedgoodasin  WHERE good_asin = \'%s\' ' % product_id
        res2 = self.mysql.select(sql)
        if (res1 or res2):
            return True
        else:
            return False


    # 商品数据采集
    def clawer(self, product_link):
        try:

            print('正在采集商品：', product_link)
            # time.sleep(random.randint(1,3))
            html = self.__request__(product_link)
            product_info = self.__parseProduct__(html,product_link)
            # 查询库中是否有该商品的数据
            flag = self.__query_product__(product_info['product_id'])
            if not flag:
                product_info = self.__save_img__(product_info)
                self.product_info_queue.put(product_info)
            else:
                print('商品已存在：{product_id:%s}' % product_info['product_id'])

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


    # 保存图片
    def __save_img__(self, product_info):
        try:
            product_id = product_info['product_id']
            # 主体图片
            product_img_list = product_info['img_url_list']
            # 变体数据
            attr_data_list = product_info['attr_data_list']
            map_img_list = product_info['map_img_list']
            # 描述
            description = product_info['description']
            description_img = product_info['description_img']

            # dir = os.getcwd().replace('spider1', '') + '/static/media/img/'
            dir = os.getcwd().replace('utils','') +  '/amazon1/amazon/amazon/static/media/img/' + str(product_id) + '/'
            if not os.path.exists(dir):
                os.makedirs(dir)

            # 主体
            img_list = []
            img_i = 0
            # img_list.append({'img_url': product_img, 'img_dir': dir + '0' + '.jpg'})
            for img_url in product_img_list:
                img_i += 1
                img_dir = dir + str(img_i) + '.jpg'
                img_list.append({'img_url': img_url, 'img_dir': img_dir})

            # product_info['img_dir'] = '/static' + img_list[0]['img_dir'].split('static')[1]
            product_info['img_dir'] = '/static/media/img/' + str(product_id) + '/1.jpg'

            # 变体
            att_img_list = []
            for att_data in attr_data_list:
                attr_id = att_data['attr_id']
                img_dir = dir + attr_id + '.jpg'
                for item in map_img_list:
                    if attr_id in item['attr_ids']:
                        img_url = item['img_url']
                        break
                att_img_list.append({'img_url':img_url,'img_dir':img_dir})
                att_data['img_url'] = img_url
                att_data['img_dir'] = '/static' + img_dir.split('static')[1]

            # 描述
            i = 0
            dec_img_list = []
            for desc_img in description_img:
                i += 1
                img_dir = dir + 'desc_' + str(i) + '.jpg'
                img_dir_sub = '/static' + img_dir.split('static')[1]
                description = description.replace(desc_img,img_dir_sub)
                dec_img_list.append({'img_url':desc_img,'img_dir':img_dir})
            product_info['description'] = description


            # 设置超时时间为30s(解决下载不完全问题且避免陷入死循环)
            socket.setdefaulttimeout(30)
            for img_data in (img_list + att_img_list + dec_img_list):
                img_url = img_data['img_url']
                img_dir = img_data['img_dir']
                try:
                    if not os.path.exists(img_dir):
                        urllib.request.urlretrieve(img_url, img_dir)
                except:
                    count = 1
                    while count <= 5:
                        try:
                            if not os.path.exists(img_dir):
                                urllib.request.urlretrieve(img_url, img_dir)
                            break
                        except:
                            err_info = '__save_img__ reloading for %d time' % count if count == 1 else '__save_img__ reloading for %d times' % count
                            print(err_info)
                            count += 1
                    if count > 5:
                        print("__save_img__ job failed!")
                        print(img_url)
                        print(product_info['product_url'])


            return product_info

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


    def run(self):

        print('启动：', self.threadName)
        while not flag_clawer:
            try:
                product_link = self.product_link_queue.get(timeout=3)
            except:
                time.sleep(3)
                continue
            self.clawer(product_link)
        print('退出：', self.threadName)
        self.mysql.close()


class ThreadParse(Thread):

    def __init__(self, i, user_id, product_info_queue, product_total, url, source):
        Thread.__init__(self)
        self.user_id = user_id
        self.source = source
        self.url = url
        self.product_total = product_total
        self.mysql = MysqlDb()
        self.threadName = '解析线程' + str(i)
        self.product_info_queue = product_info_queue

    # 将商品的排名信息写入排名表
    def __save_categorySalesRank__(self, productId, categorySalesRank, type):
        '''
        :param productId: 商品ID(goods表中id)
        :param categorySalesRank: 商品排名信息，list类型（[(排名1，类别1),(排名2，类别2)...]）
        :return:
        '''
        try:
            if type == 1:
                sql = 'insert ignore into amazonshop_categoryrank (good_id, ranking, sort) values (%s, %s, %s)'
                # sql = 'insert into amazonshop_categoryrank (good_id, ranking, sort) SELECT %s,\'%s\',\'%s\'  FROM  dual' \
                #       ' WHERE  NOT  EXISTS (SELECT id FROM amazonshop_categoryrank WHERE good_id = %s AND sort = \'%s\' )' % ()

            elif type == 2:
                sql = 'insert ignore into amazonshop_attrcategoryrank (good_attr_id, ranking, sort) values (%s, %s, %s)'

            value = []
            for data in categorySalesRank:
                value.append((productId,) + data)
            self.mysql.insert(sql, value)
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 将属性及属性值信息写入属性表（属性分类表、属性分类值表）
    def __save_dimensions__(self, dimension, dimensionValues):
        '''
        :param dimension: 商品的属性名称（如color、size）,str类型
        :param dimensionValues: 商品的属性值（如color的属性值有red、black、white）,list类型([])
        :return: 返回属性值的id（属性分类值表的id），list类型
        '''
        try:
            if 'Size' in dimension:
                export_name = 'size'
            elif 'Color' in dimension:
                export_name = 'color'
            elif 'Length' in dimension:
                export_name = 'size'
            elif 'Width' in dimension:
                export_name = 'size'
            elif 'Height' in dimension:
                export_name = 'size'
            else:
                export_name = ''

            # 写入属性信息
            sql = 'insert into amazonshop_attrcategory (attr_name,export_name) select \"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT id from amazonshop_attrcategory WHERE attr_name = \"%s\" ) ' % (
                dimension, export_name, dimension)
            cur = self.mysql.mysql.cursor()
            cur.execute(sql)
            cur.execute('commit')

            # 写入属性值信息
            sql = 'SELECT id FROM amazonshop_attrcategory WHERE attr_name = \"%s\" ' % dimension
            attr_id = self.mysql.select(sql)[0]['id']
            value = [(attr_id, dimensionValues)]
            sql = 'insert ignore into amazonshop_attrcategoryvalue (attrcategory_id, attr_value) values (%s,%s)'
            self.mysql.insert(sql, value)
            sql = 'SELECT id FROM amazonshop_attrcategoryvalue WHERE attrcategory_id = \"%s\" AND attr_value = \"%s\" ' % (attr_id, dimensionValues)
            attr_value_id = self.mysql.select(sql)[0]['id']

            # 关闭游标
            cur.close()
            return attr_value_id

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 将属性值组合信息写入商品属性表
    def __save_dimensionValues__(self, productId, product_info):
        '''
        :param productId: 商品ID（goods表中的id）
        :param product_info: 商品变体的信息，dict类型
        :return:
        '''
        try:

            # 将商品的属性值组合信息写入商品属性表
            sql = 'insert ignore into amazonshop_goodsattr (good_attr,good_id,ASIN,brand_name,seller_volume,product_name,price,product_description,img_url,img_dir,good_url,source_id) values ' \
                  '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            value = []

            attr_data_list = product_info['attr_data_list']
            for attr_data in attr_data_list:
                attr_tuple = ()
                attrs = attr_data['traitValuesMap']
                for attr_name, attr_value in attrs.items():
                    attr_value_id = self.__save_dimensions__(attr_name, attr_value)
                    attr_tuple += (attr_value_id,)

                value.append((str(attr_tuple), productId, attr_data['attr_id'],product_info['brand_name'],1,
                          product_info['product_name'],attr_data['price'],product_info['description'],attr_data['img_url'],
                          attr_data['img_dir'],product_info['product_url'],self.source))

            self.mysql.insert(sql, value)


        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 将商品信息写入商品表
    def __save_productInfo__(self, product_info, user_id):
            '''
            :param product_info: 商品信息
            :return: 返回商品ID
            '''
            try:

                sql = 'insert ignore into amazonshop_goods (ASIN,seller_volume,brand_name,product_name,price,product_description,user_id,img_url,img_dir,good_url,source_id) values ' \
                      '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                value = [(product_info['product_id'],1,product_info['brand_name'],product_info['product_name'],product_info['price'],product_info['description'],
                          user_id,product_info['img_url_list'][0],product_info['img_dir'],product_info['product_url'],self.source)]
                self.mysql.insert(sql, value)

                sql = 'select id from amazonshop_goods WHERE  ASIN = \'%s\' AND  user_id = %s ' % (product_info['product_id'], user_id)
                productId = self.mysql.select(sql)[0]['id']

                return productId

            except Exception as err:
                mylog.logs().exception(sys.exc_info())
                traceback.print_exc()

    # 保存商品数据
    def __save_data__(self, product_info):
        try:
            # 保存主体商品信息，并返回商品id
            productId = self.__save_productInfo__(product_info, self.user_id)

            # 保存变体商品信息
            self.__save_dimensionValues__(productId, product_info)

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


    def __save_process__(self, num):

        # 更新数据库的采集进度
        sql = 'update amazonshop_usershopsurl set sum = %s, num = %s WHERE  shop_url = %s'
        self.mysql.update(sql,[(self.product_total, num, self.url)])

        # 更新当前的采集进度（web展示）
        content = {"shop_url":self.url,"total":self.product_total,"number":num,"user_id":self.user_id}
        # file_root = os.getcwd() + '/file/'
        file_root = os.getcwd().replace('utils','') + '/amazon1/amazon/amazon/static/file/'
        if not os.path.exists(file_root):
            os.makedirs(file_root)
        file_path = file_root + 'process.json'
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(content, json_file, ensure_ascii=False)
        pass


    def run(self):

        try:
            print('启动：', self.threadName)
            while not flag_parse:
                try:
                    product_info = self.product_info_queue.get(timeout=3)
                except:
                    time.sleep(3)
                    continue

                try:
                    self.__save_data__(product_info)
                    print('写入商品：', product_info['product_id'], product_info['product_name'])
                    # 保存采集进度
                    global sum, num
                    num += 1
                    self.__save_process__(num)
                except:
                    pass

            print('退出：',self.threadName)
            self.mysql.close()

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


class GetAllProductsLink():

    def __init__(self, url, product_link_queue):
        '''
        :param url: 店铺链接
        :param product_link_queue: 商品链接队列
        '''
        self.url = url
        self.product_link_queue = product_link_queue

    # 采集
    def __clawer__(self, store_url):

        try:

            # time.sleep(random.randint(1, 3))
            html = self.__request__(store_url)
            self.__getProductlink__(html)
            next_url = self.__getNextPage__(html)

            return next_url

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 通过requests请求数据
    def __request__(self, store_url):
        try:

            headers = {
                "Host": "www.ebay.com",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
                "User-Agent": get_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": store_url,
                "Accept-Language": "zh-CN,zh;q=0.9",
                # "Cookie": "__gads=ID=4800ae0f6808deac:T=1566978230:S=ALNI_MaTlGIi35S1nvduTEx0cLBEu5JKlA; cid=AeV4IHdPND6uL3US%231655155015; ak_bmsc=4DCC408129C23C15D097EE4D6ED76FBA17D2D7BC5C49000030C76C5D5C7F2937~pl011yvcKmF/e6OPmIFJfH96Ajw70tFV7gahIRFNvBbANKHEmTP6xYBuvsyiD/0ntMR1dGUCfpifglhCFmhhPBqOBSUOIgVAZk40YtINBwNiroC/RBQgLx0BPHbctnTntE0zovMQKbN/SU0EcNrXEivy6K0jBYCY2cRc87CUBjX/U+ucJ1Gf7WsWRVapEAfqWutLoZ3rF5TH/4CkwcQYAEZsnyszI7Sar50l+21JoRBt0=; JSESSIONID=AA1E88CA3C8FA157AF7F6E1AA29B0D16; bm_sv=3C487E46949060362FC46EC129CD394A~D48vM8V/23G08UsD/OKzhkfFqUdQhdArTt+biadk+e/DzuoXHmBmAsONdf3SjKX/0xsMyjjIiBgy9bEFoUp7O+TGkIQli7JLwIEV81XFa5KJzy+vnQRnme1y53YA10hc6jONCCf89MHkfhUC8R5yqbAdnsHmGjJpwfeQYlMZXaI=; npii=btguid/d729537216c0a9cba0175b41fffb6bee612f346a^cguid/d7295e4416c0a4d129329faae979544c612f346a^; ns1=BAQAAAWzqHwjqAAaAANgATF9OAQ5jNzJ8NjAxXjE1NjczOTA3MTkzNTReXjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1dZDIMoMuws09DpPFbCOQUOYuFQw*; dp1=btzo/-1e05d6cdb9e^u1p/QEBfX0BAX19AQA**5f4e010e^bl/CN612f348e^pbf/%23e000e000008100020000005f4e010e^; s=CgAD4ACBdbh8OZDcyOTUzNzIxNmMwYTljYmEwMTc1YjQxZmZmYjZiZWUA7gBFXW4fDjE0Bmh0dHBzOi8vd3d3LmViYXkuY29tL3N0ci9CVVktQ09PTC1TVFVGRi0xMD9fdHJrc2lkPXAyMDQ3Njc1LmwyNTYzByCvnAA*; nonsession=BAQAAAWzqHwjqAAaAAAgAHF2UWo4xNTY3NDExNTEzeDE4MjkzODgxMDkxM3gweDJOADMABl9OAQ41MTgwMDAAywACXWzUljI1AMoAIGbSzw5kNzI5NTM3MjE2YzBhOWNiYTAxNzViNDFmZmZiNmJlZdPnJ70Ap5vRZdIlA5f5pZHUefrw; ds2=sotr/b9YGZz13l27G^; ebay=%5Edv%3D5d6caac8%5Esbf%3D%2310000000100%5Ejs%3D1%5Epsi%3DA6oLb2o8*%5E"
            }


            try:
                res = requests.get(store_url, headers=headers, verify=False ,timeout=30)
            except:
                count = 1
                while count <= 5:
                    try:
                        res = requests.get(store_url, headers=headers, verify=False ,timeout=30)
                        break
                    except:
                        err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__request__ job failed!")
                    return

            return res.text

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 获取商品链接
    def __getProductlink__(self, html_source):
        try:
            html = etree.HTML(html_source)
            protuct_links = html.xpath('//li[contains(@id,"-items")]/div/div[1]/div/a/@href')

            for protuct_link in protuct_links:
                self.product_link_queue.put(str(protuct_link))

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 获取下一页链接
    def __getNextPage__(self, html_source):
        try:
            html = etree.HTML(html_source)
            # 获取下一页url
            next_ = html.xpath('//a[@rel="next"]/@href')
            if next_:
                next_url = str(next_[0])
                if next_url == '#':
                    return False
                return next_url
            else:
                # 下一页不存在
                return False
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def run(self):
        '''
        :return: 返回店铺的所有商品链接
        '''
        # 店铺总链接（默认店铺的第一页链接）
        # print('正在采集店铺：', self.url)
        try:
            next_url = self.url
            # 循环遍历店铺的所有商品页
            for i in range(1000):
                if next_url:
                    if next_url == 'Sorry':
                        print('获取链接失败！')
                        break
                    print('正在获取该页面下的所有商品链接：', next_url)
                    next_url = self.__clawer__(next_url)
                else:
                    print('已获取店铺所有商品的链接！')
                    break

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()


def update_process():
    # 更新当前的采集进度（web展示）
    content = {}
    file_root = os.getcwd().replace('utils', '') + '/amazon1/amazon/amazon/static/file/'
    if not os.path.exists(file_root):
        os.makedirs(file_root)
    file_path = file_root + 'process.json'
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(content, json_file, ensure_ascii=False)
    pass


def get_useragent():
    useragent_list = [
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    return random.choice(useragent_list)
    pass


# 采集是否完成的标志
flag_clawer = False
# 解析是否完成的标志
flag_parse = False
# 创建日志
mylog = Mylog('clawer_ebay')
# 商品采集数
num = 0


def main(url,user_id,source):

    # 商品链接队列
    product_link_queue = Queue()
    # 商品信息队列
    product_info_queue = Queue()

    # url = 'https://www.ebay.com/itm/Case-For-Apple-iPhone-6s-7-8-Plus-X-XS-Max-Genuine-Original-Hard-Silicone-Cover/293073656062?hash=item443c8d18fe:m:mjP5S80B-ZupIIfDu8v_XTg&var=591910727889'
    # product_link_queue.put(url)
    #
    get_all_products_link = GetAllProductsLink(url, product_link_queue)
    get_all_products_link.run()


    # 商品总数
    product_total = product_link_queue.qsize()

    if not product_link_queue.empty():

        # 存储5个采集线程的列表集合
        threadcrawl = []
        for i in range(5):
            thread = ThreadClawerWish(i, product_link_queue, product_info_queue, user_id)
            thread.start()
            threadcrawl.append(thread)

        # 存储1个解析线程
        threadparse = []
        for i in range(1):
            thread = ThreadParse(i, user_id, product_info_queue, product_total, url, source)
            thread.start()
            threadparse.append(thread)

        # 等待队列为空，采集完成
        while not product_link_queue.empty():
            pass
        else:
            global flag_clawer
            flag_clawer = True

        for thread in threadcrawl:
            thread.join()

        #等待队列为空，解析完成
        while not product_info_queue.empty():
            pass
        else:
            global flag_parse
            flag_parse = True



        for thread in threadparse:
            thread.join()

        # 更新采集进程，web显示进度
        update_process()

        print('数据采集完成！')
        flag_clawer = False
        flag_parse = False

    else:
        print('数据采集失败！')


if __name__ == '__main__':

    url = 'https://www.ebay.com/str/BUY-COOL-STUFF-10?_trksid=p2047675.l2563' #19
    # url = 'https://www.ebay.com/str/ouliya0808?_trksid=p2047675.l2563' #2

    main(url,user_id=2,source=4)

