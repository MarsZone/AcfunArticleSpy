# -*- coding:utf-8 -*-
from __future__ import division
import random
import socket
import urllib
import urllib2
import re
from bs4 import BeautifulSoup
from StringIO import StringIO
import gzip
import sys
import time
from progressbar import *
from time import sleep
import json
import MySQLdb
import torndb

reload(sys)
sys.setdefaultencoding('utf-8')

widgets_Process = ['Process: ', Percentage(), ' ', Bar(marker='0', left='[', right=']'), ' ', ETA(), ' ',
                   FileTransferSpeed()]

mysql_settings = {
    'host': 'localhost:3306',
    'user': '',
    'password': '',
    'database': 'acfun'
}
db = torndb.Connection(**mysql_settings)

totalLoaded = 0


def set_proxy():
//代理
    proxy_list = [
        {'http': '58.211.13.26:55336'},  # 江苏省苏州市 电信
        {'http': '61.179.110.8:8081'},  # 山东省烟台市 联通
        {'http': '61.174.10.22:8080'},  # 浙江省金华市 电信
        {'http': '61.235.125.26:81'},

    ]
    index = random.randint(0, len(proxy_list)-1)
    #print index
    proxy = proxy_list[index]
    #proxy = {'http': '61.235.125.26:81'}
    print 'Proxy:'+ str(proxy)
    proxy_support = urllib2.ProxyHandler(proxy)
    # opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler(debuglevel=1))
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)


def get_soup(url):
    try:
        #set_proxy()
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 '
        headers = {'User-Agent': user_agent}
        request = urllib2.Request(url, headers=headers)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            data = f.read()
            soup = BeautifulSoup(data, 'lxml')
            return soup
    except urllib2.URLError, e:
        if hasattr(e, "code"):
            print e.code
        if hasattr(e, "reason"):
            print e.reason
        return 0


def get_json_str(url):
    try:
        #set_proxy()
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 '
        headers = {'User-Agent': user_agent}
        request = urllib2.Request(url, headers=headers)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request, timeout=10)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            data = f.read()
            #print response.info()
            return data
    except urllib2.URLError, e:
        if hasattr(e, "code"):
            print 'code ' + str(e.code)
            if e.code == 504:
                return 0
        if hasattr(e, "reason"):
            print 'reason ' + str(e.reason)
        return 0
    except socket.timeout, e:
        print 'Get Url Time Out.'
        return 0


def get_json_str(url):
    try:
        set_proxy()
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 '
        headers = {'User-Agent': user_agent}
        request = urllib2.Request(url, headers=headers)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request, timeout=10)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            data = f.read()
            #print response.info()
            return data
    except urllib2.URLError, e:
        if hasattr(e, "code"):
            print 'code ' + str(e.code)
            if e.code == 504:
                return 0
        if hasattr(e, "reason"):
            print 'reason ' + str(e.reason)
        print 'Retry.....'
        return get_json_str(url)
    except socket.timeout, e:
        print 'Get Url Time Out.'
        print 'Retry.....'
        return get_json_str(url)


def wait(second, tips):
    widgets = ['Load ' + tips + ' Wait: ' + str(second / 10) + 's.', Percentage(), ' ',
               Bar(marker='0', left='[', right=']'), ' ', ETA(), ' ',
               FileTransferSpeed()]
    pbar = ProgressBar(maxval=second, widgets=widgets)
    pbar.start()
    for i in range(second):
        time.sleep(0.1)
        pbar.update(i)
    pbar.finish()


def get_user_data_total_byIndex():
    total_query = db.query('SELECT count(*) as total FROM `user_list` WHERE 1')
    total_count = total_query[0]['total']
    startIndex = 0
    while startIndex < total_count:
        startIndex += 1
        query = "SELECT `userID` FROM `user_list` ORDER BY `userID` ASC LIMIT " + str(startIndex) + ",1"
        user_list = db.query(query)
        user_dicts = []
        last_ip = db.query("SELECT `lastLoginIp` FROM `user_list` where `userID`=%s",user_list[0].userID)
        if not last_ip[0].lastLoginIp:
            for user_id in user_list:
                uid = int(user_id['userID'])
                if uid > startIndex:
                    url = "http://acfun.tudou.com/usercard.aspx?uid=" + str(uid)
                    # url = "http://acfun.tudou.com/usercard.aspx?uid=53462"
                    print 'Cur StartIndex:' + str(startIndex)
                    print url
                    user_json = get_json_str(url)
                    data = json.loads(user_json)
                    success = data['success']
                    if success:
                        level = int(data['userjson']['level'])
                        if 'sign' in data['userjson']:
                            sign = data['userjson']['sign']
                        else:
                            sign = ""
                        follows = int(data['userjson']['follows'])
                        lastLoginDate = data['userjson']['lastLoginDate']
                        posts = int(data['userjson']['posts'])
                        followed = int(data['userjson']['followed'])
                        lastLoginIp = data['userjson']['lastLoginIp']
                        fans = int(data['userjson']['fans'])
                        regTime = data['userjson']['regTime']
                        gender = data['userjson']['gender']
                        user_dict = {
                            'uid': uid,
                            'level': level,
                            'sign': sign,
                            'follows': follows,
                            'lastLoginDate': lastLoginDate,
                            'posts': posts,
                            'followed': followed,
                            'lastLoginIp': lastLoginIp,
                            'fans': fans,
                            'regTime': regTime,
                            'gender': gender,
                        }
                        user_dicts.append(user_dict)
                    print 'Load Finished.'
                    wait(random.randint(40, 150), 'Next User.')
            for udict in user_dicts:
                uid = udict['uid']
                db.update("UPDATE `user_list` SET `level`= %s,`sign`=%s,`follows`=%s,`"
                          "lastLoginDate`=%s,`posts`=%s,`followed`=%s,"
                          "`lastLoginIp`=%s,`fans`=%s,`regTime`=%s,"
                          "`gender`=%s WHERE userID = %s",
                          udict['level'], udict['sign'], udict['follows'], udict['lastLoginDate'], udict['posts'],
                          udict['followed'], udict['lastLoginIp'], udict['fans'], udict['regTime'], udict['gender'], uid)
            print 'Data write to db.'


# for d in dict:
#     print json.dumps(d,encoding="GBK",ensure_ascii=False)
# start = time.time()
# print("Start.")
# get_channel_data(110)
# end = time.time()
# print '110 Load Finished time passed:' + str((end - start))
# print 'Total Loaded Article:' + str(totalLoaded)
# get_channel_data(73)
# end = time.time()
# print '73 Load Finished time passed:'+ str(end - start)
#set_proxy()

#旧方法,从指定index位置往后慢慢爬用户数据...新方法要遍历数据库,找到木有ip的再爬.
get_user_data_total_byIndex()
