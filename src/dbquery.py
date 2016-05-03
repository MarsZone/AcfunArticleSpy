# coding=utf-8
import torndb
import sys
import time
import json
import os
import urllib2, urllib
from progressbar import *
from time import sleep
from ipip import IP

reload(sys)
sys.setdefaultencoding('utf-8')

mysql_settings = {
    'host': 'localhost:3306',
    'user': '',
    'password': '',
    'database': 'acfun'
}

db = torndb.Connection(**mysql_settings)

IP.load(os.path.abspath("17monipdb.dat"))


#
def get_user_rank():
    # Get User post Count
    print("Start Get user post count from db.")
    startDate = (time.strftime("%Y-%m-%d"))
    endDate = (datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d'))
    channelCount = db.query('SELECT `userID`,`userName` ,count(*) AS count FROM '
                            '(SELECT * FROM `comment_list` WHERE Date(`postDate`) '
                            'between %s and %s ORDER BY `comment_list`.`postDate` DESC)sort '
                            'GROUP BY userID ORDER BY count(*) DESC LIMIT 0,50', endDate, startDate)
    json_str = json.dumps(channelCount)
    json_str = 'userRank("' + startDate + '","' + endDate + '",' + json_str + ');'
    
    print("Post to server.")
    mydata = [('json_str', json_str)]  # The first is the var name the second is the value
    mydata = urllib.urlencode(mydata)
    path = 'http://www.ezlands.com/acm/data/Receive.php'  # the url you want to POST to
    req = urllib2.Request(path, mydata)
    req.add_header("Content-type", "application/x-www-form-urlencoded")
    page = urllib2.urlopen(req).read()
    print page
    print("User rank data updated.")


def send_quote_to_user():
    startDate = (time.strftime("%Y-%m-%d"))
    endDate = (datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(60), '%Y-%m-%d'))
    print startDate
    print endDate
    uid = 0
    email = ""
    name = ""
    //path is aim to get member state.
    path = 'http://www.ezlands.com/acm/comment/gml.php'
    req = urllib2.Request(path)
    response = urllib2.urlopen(req).read()
    response = json.loads(response)
    # print response[0]['email']
    # member_list = db.query("SELECT * FROM `member_list` WHERE 1")
    # for member in member_list:
    for member in response:
        print member['uid']
        print member['email']
        print member['active']
        if member['active'] == "1":
            content_body = "To:"
            user_name = db.query("SELECT `userName` FROM `user_list` WHERE `userID` = %s", member['uid'])
            if user_name:
                print user_name[0].userName
                content_body = content_body + user_name[0].userName + '<br/>'
                content_body += "日期范围:" + endDate + "-To-" + startDate + "<br/>"
                user_comment = db.query(
                    "SELECT * FROM `comment_list` WHERE `userID` = %s AND Date(`postDate`) between %s and %s",
                    int(member['uid']),
                    endDate,
                    startDate)
                print user_comment
                counter = 0
                for comment in user_comment:
                    # print "---------------S------------------"
                    # print "CommentID:" + str(comment.cid)
                    # print "Comment_Content:" + str(comment.content.encode("GBK"))
                    all_quote = db.query("SELECT * FROM `comment_list` WHERE `quoteID` = %s", comment.cid)
                    if all_quote:
                        for quote in all_quote:
                            # print "QuoteName:" + quote.userName
                            # print "QuoteContent:" + quote.content.encode("GBK")
                            # print "QuoteCID:" + str(quote.cid)  # save the cid to the mail_list
                            # search quote cid from db
                            mailed_cid = db.query("select * from `maild_list` where `cid`=%s", quote.cid)
                            if not mailed_cid:
                                content_body = content_body + "<br/>评论日期:" + comment.postDate + "<br/>"
                                content_body = content_body + "评论内容:" + comment.content + '<br/>'
                                print mailed_cid
                                counter += 1
                                content_body = content_body + "*回复时间:" + quote.postDate + "<br/>"
                                content_body = content_body + "__引用人:" + quote.userName + "<br/>"
                                content_body = content_body + "__引用内容:" + quote.content + "<br/>"
                                # content_body = content_body + "<a herf=http://acfun.tudou.com/v/ac"+str(quote.aid)+"#layer="+str(quote.count)+";>点击跳转</a><br/>"
                                content_body += '<span style="color: rgb(34, 34, 34); font-family: Consolas, "Lucida Console", monospace; font-size: 12px; line-height: normal; white-space: pre-wrap;">'
                                content_body = content_body + "<a href=http://acfun.tudou.com/v/ac" + str(
                                    quote.aid) + "#layer=" + str(quote.count) + ";>点击跳转</a><br/></span>"

                                # save quote cid to db
                                try:
                                    db.insert("INSERT INTO `maild_list` (`cid`, `aid`,`content`) VALUES (%s, %s,%s);",
                                              quote.cid,
                                              quote.aid,
                                              quote.content)
                                except Exception, e:
                                    print 'Error:%s' % e
                        print content_body
                if counter > 0:
                    post_data = [('from', 'ezlands.com'), ('to', member['email']), ('subject', '新的召唤'),
                                 ('body', str(content_body))]
                    post_data = urllib.urlencode(post_data)
                    path = 'http://www.ezlands.com/Mail/acmail.php'
                    req = urllib2.Request(path, post_data)
                    page = urllib2.urlopen(req).read()
                    print page
    return 0


# send_quote_to_user()
# get_user_rank()
def get_user_location():
    user_data = db.query("select * from `user_list`")
    country_list = []
    other_list = []
    province_list = []
    for user in user_data:
        if user.lastLoginIp:
            ip = str(user.lastLoginIp).replace("*", "1")
            row = IP.find(ip)
            row = str(row).split('\t')
            country = ""
            province = ""
            if len(row) > 2:
                country = str(row[0])
                province = str(row[1])
            # print "country:" + country
            # print "province:" + province
            if country == "中国":
                country_list.append(country)
                province_list.append(province)
            elif country != "":
                other_list.append(country)

    diction_list = []
    myset = set(province_list)
    for item in myset:
        print item.encode("GBK") + ":" + str(province_list.count(item))
        d = {"name": item, "value": str(province_list.count(item))}
        diction_list.append(d)

        # for list in other_list:
        #     print list.encode("GBK")
    json_str = json.dumps(diction_list)
    json_str = 'chinaMap(' + json_str + ');'
    print json_str
    with open('xxxx/mapdata.js', 'w') as outfile:
        outfile.write(json_str)
    outfile.closed

    diction_list_world = []
    worldSet = set(other_list)
    for item in worldSet:
        print item.encode("GBK") + ":" + str(other_list.count(item))
        d = {"name": item, "value": str(other_list.count(item))}
        diction_list_world.append(d)
        if item =="局域网":
            d = {"name": "猴山_冰岛_丧失岛", "value": str(other_list.count(item))}
            diction_list_world.append(d)
    d = {"name": "中国", "value": str(len(province_list))}
    diction_list_world.append(d)
    json_str_world = json.dumps(diction_list_world)
    json_str_world = 'world_Map(' + json_str_world + ');'
    print json_str_world
    with open('xxxx/world_mapdata.js', 'w') as outfile:
        outfile.write(json_str_world)
    outfile.closed
    # json_str_world = json.dump(diction_list_world)
    # json_str_world = 'worldMap(' + json_str + ');'
    # print json_str
    # with open('xxxx/mapdata.js', 'w') as outfile:
    #     outfile.write(json_str)
    # outfile.closed

get_user_location()
#send_quote_to_user()
