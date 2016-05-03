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
import dbquery

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
    proxy_list = [
        {'http': '61.179.110.8:8081'},  # 山东省烟台市 联通
        {'http': '61.174.10.22:8080'},  # 浙江省金华市 电信
        {'http': '60.191.174.13:3128'},  # 浙江省台州市仙居县 电信
        {'http': '60.194.100.51:80'},  # 北京市 电信通
        {'http': '61.235.125.26:81'},
    ]
    index = random.randint(0, len(proxy_list) - 1)
    # print index
    proxy = proxy_list[index]
    # proxy = {'http': '58.251.47.101:8081'}
    print 'Proxy:' + str(proxy)
    proxy_support = urllib2.ProxyHandler(proxy)
    # opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler(debuglevel=1))
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)


def get_soup(url):
    try:
        # set_proxy()
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
        # set_proxy()
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 '
        headers = {'User-Agent': user_agent}
        request = urllib2.Request(url, headers=headers)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request, timeout=10)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            data = f.read()
            # print response.info()
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


def start_progress(url, href, channel):
    requestUrl = url
    # requestUrl = "http://acfun.tudou.com/a/ac2485653"
    # requestUrl = "http://acfun.tudou.com/a/ac2563055"

    a_soup = get_soup(requestUrl)
    title = a_soup.find(id='title_1')
    if title is None:
        return 0;
    titleStr = title.find("span", class_='txt-title-view_1').contents[0]
    try:
        print 'Start:' + titleStr + " Loading."
    except Exception, e:
        print 'Error:%s' % e
    # print 'Start:' + titleStr.encode("GBK") + " Loading."
    # print titleStr.encode("GBK")
    # wait(20)
    aid = re.findall('\d+', href)
    print 'aid:' + str(aid[0])
    # title = titleStr
    if a_soup.find(id='block-data-view'):
        a_data_time = a_soup.find(id='block-data-view')['data-date']
        uid = a_soup.find(id='block-data-view')['data-uid']
    else:
        return 0;
    uname = a_soup.find(id='block-data-view')['data-name']
    wait(random.randint(20, 50), 'ArticleInfo')
    content_count = get_json_str(
        'http://acfun.tudou.com/content_view.aspx?contentId=' + str(aid[0]) + '&channelId=' + str(channel))
    # print 'datetime:' + a_data_time
    # print "uid:" + uid
    # print "uname:" + uname
    # print "views:" + views
    # print "replies:" + replies

    wait(random.randint(20, 50), 'Content Page:1')
    requestUrl = 'http://acfun.tudou.com/comment_list_json.aspx?contentId=' + str(aid[0]) + '&currentPage=1'
    rawStr = get_json_str(requestUrl)
    json_data = json.loads(rawStr)
    total_page = int(json_data["data"]["totalPage"])
    page = int(json_data["data"]["page"])

    if content_count != 0:
        content_count = content_count[1:-1]
        content_count = content_count.split(',')
        views = content_count[0]
        replies = content_count[1]
    else:
        ifHasDict = db.query('select aid from article_list where aid = %s', str(aid[0]))
        if ifHasDict:
            view_list = db.query('select views from article_list where aid = %s', str(aid[0]))
            views = view_list[0]['views']
        else:
            views = 0
            replies = int(json_data["data"]["totalCount"])

    article_dicts = []
    A_dict_row = {
        'aid': str(aid[0]),
        'title': titleStr,
        'datetime': a_data_time,
        'views': views,
        'replies': replies,
        'uid': uid,
        'uname': uname,
        'channel': channel
    }
    article_dicts.append(A_dict_row)
    for dict in article_dicts:
        ifHasDict = db.query('select aid from article_list where aid = %s', dict['aid'])
        if ifHasDict:
            db.insert_by_dict("article_list", dict, True)
        else:
            db.insert_by_dict("article_list", dict)

    keep_data(json_data, str(aid[0]))
    print 'Page:' + str(page) + ' write to db.'
    page += 1
    while page < (total_page + 1):
        wait(random.randint(100, 200), 'Content Page:' + str(page))
        requestUrl = 'http://acfun.tudou.com/comment_list_json.aspx?contentId=' + str(aid[0]) + '&currentPage=' + str(
            page)
        rawStr = get_json_str(requestUrl)
        json_data = json.loads(rawStr)
        keep_data(json_data, str(aid[0]))
        print 'Page:' + str(page) + ' write to db.'
        page += 1
    try:
        print 'End:' + titleStr.encode("GBK") + "Read Finished"
    except Exception, e:
        print 'Error:%s' % e


def keep_data(json_data, a_id):
    UserDicts = []
    CommentDicts = []
    cid = 0
    quoteId = 0
    content = ""
    postDate = ""
    userID = 0
    userName = ""
    userImg = ""
    count = 0
    deep = 0
    aid = a_id
    CommentContentDic = json_data["data"]["commentContentArr"]
    for key in CommentContentDic.keys():
        cid = int(CommentContentDic[key]['cid'])
        quoteId = int(CommentContentDic[key]['quoteId'])
        content = CommentContentDic[key]['content']
        postDate = CommentContentDic[key]['postDate']
        userID = int(CommentContentDic[key]['userID'])
        userName = CommentContentDic[key]['userName']
        count = int(CommentContentDic[key]['count'])
        deep = int(CommentContentDic[key]['deep'])
        if cid == -1:
            cid = key[1:]
        if 'userImg' in CommentContentDic[key]:
            userImg = CommentContentDic[key]['userImg']
        else:
            userImg = "no Pic"
        C_dict_row = {
            'cid': cid,
            'quoteId': quoteId,
            'content': content,
            'postDate': postDate,
            'userID': userID,
            'userName': userName,
            'userImg': userImg,
            'count': count,
            'deep': deep,
            'aid': aid
        }
        User_dict_row = {
            'userID': userID,
            'userName': userName,
            'userImg': userImg,
            'level': 0,
            'sign': "",
            'follows': 0,
            'lastLoginDate': "",
            'posts': 0,
            'followed': 0,
            'lastLoginIp': "",
            'fans': 0,
            'regTime': "",
            'gender': 0,
        }
        if User_dict_row not in UserDicts:
            UserDicts.append(User_dict_row)
        CommentDicts.append(C_dict_row)
    # if dict:
    #     #db.update('UPDATE user_list SET userName=%s,userImg=%s WHERE userID=%s',user['userName'],user['userImg'],user['userID'])
    #     ret = db.insert_by_dict("user_list", user,True)
    # else:
    #    ret = db.insert_by_dict("user_list", user)
    for Cdict in CommentDicts:
        ifHasDict = db.query('select cid from comment_list where cid = %s', Cdict['cid'])
        if ifHasDict:
            db.insert_by_dict("comment_list", Cdict, True)
        else:
            db.insert_by_dict("comment_list", Cdict)
            # print json.dumps(Cdict,encoding="GBK",ensure_ascii=False)
    for Udict in UserDicts:
        ifHasDict = db.query('select userID from user_list where userID = %s', Udict['userID'])
        if ifHasDict:
            # db.insert_by_dict("user_list", Udict, True)
            continue
        else:
            db.insert_by_dict("user_list", Udict)
            # print json.dumps(Udict,encoding="GBK",ensure_ascii=False)


def get_channel_data(channel):
    global totalLoaded
    for i in range(1, 10):
        content = get_soup('http://acfun.tudou.com/v/list' + str(channel) + '/index_' + str(i) + '.htm').find(
            id='block-content-article')
        items = content.find_all(class_='item')
        hrefs = []
        for item in items:
            hrefs.append(item.a['href'])
            print item.a['href'].encode("GBK")

        domain = "http://acfun.tudou.com"
        # requestUrl = domain + hrefs[0]
        # start_progress(requestUrl)
        page_loaded = 0
        for href in hrefs:
            requestUrl = domain + href
            start_progress(requestUrl, href, channel)
            page_loaded += 1
            totalLoaded += 1
            print "Loading Finished:" + str(page_loaded) + "/" + str(len(hrefs))
            print 'Current index:' + str(i)
            print 'Current Channel:' + str(channel)
            print 'Current TotalLoaded:' + str(totalLoaded)
            wait(random.randint(100, 150), 'Next Article.')
        print 'Loading Page index_' + str(i) + ' Finished. Wait for next page.'
        wait(random.randint(100, 150), 'list index_' + str(i))


def load_mailed_art(channel):
    domain = "http://acfun.tudou.com"
    # requestUrl = domain + hrefs[0]
    # start_progress(requestUrl)
    page_loaded = 0
    list = db.query("select * from maild_list")
    hrefs = []
    for href in list:
        hrefs.append("/a/ac" + str(href.aid))
    for href in hrefs:
        requestUrl = domain + href
        start_progress(requestUrl, href, channel)
        page_loaded += 1
        print "Loading Finished:" + str(page_loaded) + "/" + str(len(hrefs))
        print 'Current Channel:' + str(channel)
        wait(random.randint(100, 150), 'Next Article.')
    print 'Loading Page index_' + str(href) + ' Finished. Wait for next page.'
    wait(random.randint(100, 150), 'list index_' + str(href))


# for d in dict:
#     print json.dumps(d,encoding="GBK",ensure_ascii=False)
currentLoops=0;
while True:
    #load_mailed_art(110) #有bug,会重复Load文章,要先去掉重复的文章
    start = time.time()
    print("Load Comment Start.")
    try:
        get_channel_data(110)
    except Exception, e:
        print 'Error:%s' % e
    end = time.time()
    print '110 Load Finished time passed:' + str((end - start))
    print 'Total Loaded Article:' + str(totalLoaded)
    dbquery.get_user_rank();
    dbquery.send_quote_to_user();
    currentLoops += 1
    print "Current Loops:"+str(currentLoops)
    #3-hour to 5hour
    wait(random.randint(108000, 180000), 'LoopHold.')
#dbquery.send_quote_to_user();
#dbquery.send_quote_to_user();
# get_channel_data(73)
# end = time.time()
# print '73 Load Finished time passed:'+ str(end - start)
