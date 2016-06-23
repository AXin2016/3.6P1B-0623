# encoding: utf-8
'''
Created on 2016/4/22
@author: wangguojie
'''
import os.path, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from config.db_config import *
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


class Pipeline:
    
    """ construct pipeline for MongoDB Aggregation"""
    
    def __init__(self):
        self.pipeline = []
        
    def match(self,**arg):
        self.pipeline.append({"$match":self.__recover(arg)})
        return self
    
    def group(self,**arg):
        self.pipeline.append({"$group":self.__recover(arg)})
        return self
    
    def unwind(self,**arg):
        self.pipeline.append({"$unwind":self.__recover(arg)})
        return self
        
    def project(self,**arg):
        self.pipeline.append({"$project":self.__recover(arg)})
        return self
    
    def sort(self,**arg):
        self.pipeline.append({"$sort":self.__recover(arg)})
        return self    
    
    def limit(self,**arg):
        self.pipeline.append(self.__recover(arg))
        return self  
    
    def get(self):
        return self.pipeline
    
    #  '___'    -->  '$', for first '___'
    #  '__'   -->  '.'
    def __recover(self,arg):
        for k in arg.keys():
            arg[k.replace('___','$',1).replace('__','.')] = arg.pop(k)
        return arg
    
    def replace(self, old,  new):
        return eval(str(self.pipeline).replace(old, new))


# collection: datebase's collection
# pipeline: MongDB aggregate pipeline
# keys: key need to push or addToSet
# note: this aggregation assume unique '_id'.
def aggregate(collection,pipeline,*keys):
    result_list = []
    agg_list = list(collection.aggregate(pipeline))
    if len(agg_list) > 0:
        for key in keys:
            if key in agg_list[0]:
                result_list.append(agg_list[0][key])
            else:
                result_list.append([])
    else:
        for key in keys:
            result_list.append([])
    return result_list


def unpack(iterable):
    """
    unpack a nested array
    """
    result = []
    for x in iterable:
        if type(x) is list:
            result.extend(unpack(x))
        else:
            result.append(x)
    return result


def merge_dict(*dicts):
    """
    合并一组dict
    """
    res = dict()
    for d in dicts:
        res.update(d)
    return res
    # return dict(sum(map(dict.items, dicts), []))


def to_list(a):
    return a if type(a) is list else [a]


def percent(d1, d2, s=False):
    """
    百分比
    :param d1: 分子, 可以为整数 或 数组, 如果为数组, 则分子为数组长度
    :param d2: 分母, 可以为整数 或 数组, 如果为数组, 则分母为数组长度
    :param s: 返回是否为str, 如果True, 返回格式为 25.00%, 如果为False, 返回格式为25.00
    :return: 百分比, 为str/float, 保留小数点后两位
    """
    res = 0
    if type(d1) is int and type(d2) is int:
        res = round(d1*100.0/d2, 2) if d2 else 0
    elif type(d1) is list and type(d2) is list:
        res = round(len(d1)*100.0/len(d2), 2) if len(d2) else 0
    return str(res)+'%' if s else res


def new_users(start, end, user='user', platform=None, count=False):
    """
    计算某段时间内的新增用户
    :param start: 开始时间
    :param end: 结束时间
    :param user: 注册用户为user, 移动端设备为device
    :param platform: android/ios/pc, 可以为字符串或数组; 如果为None, 则表示所有平台
    :param count: 如果只需要知道新增用户的个数则为True, 如果需要新增用户的id则为False
    :return: 返回新增用户的ids或个数
    """
    if platform is None:
        platform = {"$in": ['android', 'ios', 'pc']}
    if type(platform) is list:
        platform = {"$in": platform}
    if user == 'user':
        query = {
            "from": platform,
            "$or": [
                {"type": {"$ne": "batch"}, "registTime": {"$gte": start, "$lt": end}},
                {"type": "batch", "activateDate": {"$gte": start, "$lt": end}}]
        }
        if count:
            return users.count(query)
        else:
            user_list = list(users.find(query, {"_id": 1}))
            return [u['_id'] for u in user_list]
    elif user == 'device':
        query = {"os": platform, "activateDate": {"$gte": start, "$lt": end}}
        if count:
            return deviceAttr.count(query)
        else:
            user_list = list(deviceAttr.find(query, {"device": 1}))
            return [u['device'] for u in user_list]
    return 'ERROR'


def active_users(start, end, user='user', platform=None, input_users=None, count=False):
    """
    计算某段时间内的活跃用户
    :param start: 开始时间
    :param end: 结束时间
    :param user: 注册用户为user, 移动端设备为device
    :param platform: android/ios/pc, 可以为字符串或数组; 如果为None, 则表示所有平台
    :param input_users: 初始输入用户
    :param count: 如果只需要知道活跃用户的个数则为True, 如果需要活跃用户的id则为False
    :return: 返回活跃用户的ids或个数
    """
    if platform is None:
        platform = {"$in": ['android', 'ios', 'pc']}
    if type(platform) is list:
        platform = {"$in": platform}
    days = (end - start).days
    dates = [s.strftime("%Y%m%d") for s in [start+datetime.timedelta(hours=8+24*i) for i in range(days)]]
    query = {"daily": {"$elemMatch": {"$in": dates}}}
    query.update({"from" if user == 'user' else 'os': platform})
    collection = userAttr if user == 'user' else deviceAttr

    if input_users is not None:
        query.update({user: {"$in": input_users}})
    if count:
        active = collection.count(query)
    else:
        active = collection.distinct(user, query)
    return active


def uv_pv(start, end, keys, query=None, input_users=None, user='user', count=False, collection=events):
    """
    某个(组)事件的users, uv, pv, per(与输入用户的占比,如果有输入用户,否则为0)
    :param start: 开始时间
    :param end: 结束时间
    :param keys: 需要计算的时间, 可以为一个事件或一组事件
    :param query: 查询条件; eventValue.videoId 可以写成 _videoId; platform:{$in:['android', 'ios']} 可以写成 platform:['android', 'ios']
    :param input_users: 初始用户
    :param user: 默认为user, 如果要计算移动端设备则为device
    :param count: 如果只需返回pv uv的数值, 则count为True; 如果需要返回ids, count为False
    :param collection: 数据库collection, 默认为db.eventsV35.eventV35
    :return: {users: 输出users, uv: uv, pv: pv, per: 与输入用户占比(如果没有输入用户则为0)}
    """
    if query is None:
        query = {}
    query.update({"eventKey": keys})
    query = dict((k.replace('_', 'eventValue.'), v) for k, v in query.items())
    for q in query:
        if type(query[q]) == list:
            query[q] = {"$in": query[q]}
    if input_users is not None:
        query.update({user: {"$in": input_users}})
    else:
        query.update({user: {"$nin": inner_users}})
    pipeline = [
        {"$match": merge_dict({"serverTime": {"$gte": start, "$lt": end},
                               }, query)},
        {"$group": {"_id": "$"+user, "pv": {"$sum": 1}}},
        {"$group": {"_id": None, "users": {"$addToSet": "$_id"},  "pv": {"$sum": "$pv"}, "uv": {"$sum": 1}}}
    ]
    if count:
        del pipeline[2]["$group"]["users"]
    uu = list(collection.aggregate(pipeline, allowDiskUse=True))
    if len(uu) == 0:
        res = {"users": [], "uv": 0, "pv": 0, "per": 0}
    else:
        data = uu[0]
        res = {"users": data['users'] if not count else [], "uv": data['uv'], "pv": data['pv'], 'per': percent(data['uv'], len(input_users)) if input_users else 0}
    if count:
        del res["users"]
    return res


def funnel(start, end, funnel_steps, init_users=None, return_steps=None, user='user', collection=events):
    """
    非时序化漏斗
    :param start:  开始时间
    :param end: 结束时间
    :param funnel_steps: [
    {name: 漏斗步骤名称,
    key: 漏斗步骤eventKey(可以为字符串或数组),
    query: 其他条件(可以没有); eventValue.videoId 可以这样写: _videoId; platform:{$in:['android', 'ios']} 可以写成 platform:['android', 'ios']
    parent: 漏斗上一步(如果漏斗的上一步就是funnel_steps中的上一步,可以没有); 注意, 漏斗的第一步是1}]    :param init_users: 漏斗最开始的输入用户
    :param init_users: 初始输入用户
    :param return_steps: 需要返回漏斗步骤输出用户id的步骤列表
    :param user: 默认为'user', 但是移动端可能是'device'
    :param collection: 数据库collection, 默认为db.eventsV35.eventV35
    :return: {步骤名称, 步骤uv, 步骤pv}, 需要返回的用户id
    """
    step_users = [init_users]
    res = []
    for i, e in enumerate(funnel_steps):
        data = uv_pv(start, end, e['key'], e['query'] if 'query' in e else {}, step_users[e['parent'] if 'parent' in e else i], user=user, collection=collection)
        uu, pv, per = data['users'], data['pv'], data['per']
        step_users.append(uu)
        res.append({'name': e['name'], 'uv': len(uu), 'pv': pv, 'per': per})
    if return_steps:
        return res, list(map(step_users.__getitem__, return_steps))
    return res


def find(arr, i):
    if type(i) is list:
        res = [find(arr, j) for j in i]
        res = [r for r in res if r != -1]
        return min(res)
    return arr.index(i) if i in arr else -1


def get_query(funnel_steps):
    querys = [merge_dict({"eventKey": f['key']}, f['query'] if 'query' in f else {}) for f in funnel_steps]
    res = []
    for query in querys:
        for q in query:
            if type(query[q]) == list:
                query[q] = {"$in": query[q]}
        query = dict((k.replace('_', 'eventValue.'), v) for k, v in query.items())
        res.append(query)
    return res


def sequential_funnel(start, end, funnel_steps, init_users=None, return_steps=None, user='user', collection=events):
    """
    时序化漏斗
    :param start: 漏斗开始时间
    :param end: 漏斗结束时间
    :param funnel_steps: [
    {name: 漏斗步骤名称,
    key: 漏斗步骤eventKey(可以为字符串或数组),
    query: 其他条件(可以没有); eventValue.videoId 可以这样写: _videoId; platform:{$in:['android', 'ios']} 可以写成 platform:['android', 'ios']
    parent: 漏斗上一步(如果漏斗的上一步就是funnel_steps中的上一步,可以没有); 注意, 漏斗的第一步是1}]
    :param init_users: 漏斗输入
    :param return_steps: 返回某些步骤的输出用户, 比如 [1,2,5], 注意第一步是1; 可以为None
    :param user: 默认为'user', 但是移动端可能是'device'
    :param collection: 数据库的collection, 默认events
    :return: [(漏斗步骤, uv, 与上一步占比)], return_users(如果有return_steps的话)
    """
    # pool = multiprocessing.Pool(processes=NUM_OF_PROCESS)
    # result_list = []
    # l = len(init_users)
    # interval = int(math.ceil(l*1.0 / NUM_OF_PROCESS))
    # for i in range(NUM_OF_PROCESS):
    #     pool.apply_async(process, (START_DATE + datetime.timedelta(hours=i*interval), START_DATE + datetime.timedelta(hours=(i+1)*interval)))
    #     result_list.append(pool.apply_async(processing, (str(topic_id), user_type)))
    # pool.close()
    # pool.join()
    query = {"serverTime": {"$gte": start, "$lt": end}, "$or": get_query(funnel_steps)}
    if init_users is not None:
        query.update({user: {"$in": init_users}})
    pipeline = [
        {"$match": query},
        {"$project": {
            user: 1,
            "eventKey": 1,
            "eventTime": 1,
            "time": {"$cond": [{"$eq": ["$eventTime", '']}, {"$subtract": ["$serverTime", datetime.datetime(1970, 1, 1)]}, "$eventTime"]}
#            "time": {"$ifNull": ["$eventTime", {"$subtract": ["$serverTime", datetime.datetime(1970, 1, 1)]}]}
        }},
        {"$group": {
            "_id": "$"+user,
            "events": {"$push": {"key": "$eventKey", "time": "$time"}}
        }}
    ]
    user_events = list(collection.aggregate(pipeline, allowDiskUse=True))
    multiple_keys = dict([(e['key'][1], e['key'][0]) for e in funnel_steps if type(e['key']) is list])

    funnel_dict = OrderedDict([(e['key'] if type(e['key']) is str else e['key'][0], {"uv": 0, "pv": 0, "users": [], "name": e['name'], "parent": e['parent'] if 'parent' in e else i}) for i, e in enumerate(funnel_steps)])
    parent_list = [e['parent'] if 'parent' in e else i for i, e in enumerate(funnel_steps)]
    for i in range(1, len(parent_list)):
        if parent_list[i-1] > parent_list[i]:
            parent_list[i-1] = parent_list[i]
        i -= 1
    main_flow_index = [i-1 for i in parent_list if i != 0]
    main_flow_index = list(set(main_flow_index))
    funnel_keys = list(funnel_dict.keys())

    main_flow = list(map(funnel_keys.__getitem__, main_flow_index))
    for user_event in user_events:
        in_lists = []
        for e in user_event['events']:
            if e['key'] in multiple_keys:
                e['key'] = multiple_keys[e['key']]

        uu = sorted(user_event['events'], key=lambda k: funnel_keys.index(k['key']))
#        print(uu[0]['time'])
#        print(type(uu[0]['time']))
        uu = sorted(uu, key=lambda k: k['time'])

        flow = [u1['key'] for u1 in uu]
        i = 0
        while i < len(flow)-1:
            if flow[i] == flow[i+1]:
                del flow[i]
            else:
                i += 1
        
        first = funnel_keys[0]
        indexes = [i for i, u1 in enumerate(flow) if u1 == first]
        groups = []
        for i, ind in enumerate(indexes):
            groups.append(flow[ind: indexes[i+1] if len(indexes) > i+1 else len(flow)])
        for g in groups:
            in_list = [0]
            out_list = []
            for i, e in enumerate(g):
                if funnel_dict[e]['parent'] in in_list and all([o < i+1 for o in out_list]):
                    if funnel_keys.index(e)+1 not in in_list:
                        in_list.append(funnel_keys.index(e)+1)
                    elif funnel_keys.index(e)+1 in in_list and e in main_flow:
                        break
                else:
                    out_list.append(funnel_dict[e]['parent'])
            in_list = [i-1 for i in in_list if i != 0]
            in_lists.append(in_list)
        steps = unpack(in_lists)
        steps_counter = Counter(steps)
        for i in steps_counter:
            funnel_dict[funnel_keys[i]]['uv'] += 1
            funnel_dict[funnel_keys[i]]['pv'] += steps_counter[i]
            funnel_dict[funnel_keys[i]]['users'].append(user_event['_id'])
    data = [d[1] for d in list(funnel_dict.items())]

    res = []
    return_users = []
    for i, d in enumerate(data):
        per = percent(d['uv'],  data[d['parent']-1]['uv']) if d['parent'] != 0 else percent(d['uv'], len(init_users)) if init_users else 100
        r = {"name": d['name'], "uv": d['uv'], 'per': per, "pv": d['pv']}
        res.append(r)
        if return_steps and i in return_steps:
            return_users.append(d['users'])
    if return_steps:
        return res, return_users
    return res


def get_allowed_users(start=None, end=None, allowed=True):
    """
    获得某段时间内的开关内用户 或者所有的allowed为False的用户
    :param start: 开始时间
    :param end: 结束时间
    :param allowed: 如果开关内用户, 则True, else False
    :return: 返回某段时间内的开关内用户(allowed=True) 或者所有的allowed为False的用户(allowed=False)
    """

    if start and end:
        if start > ALLOWED_DATE_ON:
            allowed_users = users.distinct('_id', {"allowed": True, "allowedDate": {"$gte": start, "$lt": end}})
        else:
            allowed_users = users.distinct('_id', {"allowed": True, "$or": [{"allowedDate": {"$lt": end}}, {"allowedDate": {"$exists": False}}]})
    else:
        allowed_users = users.distinct('_id', {'allowed': allowed})

    allowed_users = list(set(allowed_users) - set(inner_users))
    return allowed_users


def get_coupon_users(user=None, start=None, end=None):
    """
    取得某段时间内获得体验券的用户
    :param user: 初始输入用户
    :param start: 开始时间
    :param end: 结束时间
    :return: 获得体验券的用户
    """
    query = {}
    if user is not None:
        query.update({"userId": {"$in": user}})
    if start and start > PAY_ON_DATE:
        query.update({"validity.begin": {"$gte": start}})
    if end:
        if 'validity.begin' in query:
            query['validity.begin'].update({"$lt": end})
        else:
            query.update({"validity.begin": {"$lt": end}})
    coupon_users = coupons.distinct('userId', query)
    coupon_users = list(set(coupon_users) - set(inner_users))
    return coupon_users


def get_coupon_users_from_users(user=None):
    """
    从users表中获得有体验券的用户
    :param user: 初始输入用户, 可以为None
    :return: 有体验券的用户
    """
    query = {"trialCouponTips.firstThree.got": True}
    if user:
        query.update({"_id": {"$in": user}})
    return users.distinct('_id', query)


def get_payable_chapters():
    """
    获得所有付费章节id
    """
    return list(map(str, chapters.distinct('_id', {"includeCharges": True})))


def get_payable_themes():
    """
    获得所有付费主题id
    """
    themes = list(chapters.aggregate([
        {"$match": {"includeCharges": True}},
        {"$group": {"_id": None, "themes": {"$addToSet": "$themes"}}}
    ]))
    themes = [item for sub in themes[0]['themes'] for item in sub]
    return [str(t['_id']) for t in themes if t['includeCharges'] is True]


def get_payable_topics():
    """
    获得所有付费知识点id
    """
    return list(map(str, topics.distinct('_id', {"pay": True})))


def arr_2_dict(arr, k, k2=None):
    """
    将一个由dict组成的数组变为dict
    :param arr: 输入数组, 格式为 [{'id': 'a', 'count': 1}, {'id': 'b', 'count': 2}]
    :param k: 需要作为key的字段, 比如'id'
    :param k2: 需要作为value的字段, 比如'count'; 如果为None, 作为value的是整个dict
    :return: 返回一个dict, 如{'a':1, 'b': 2}; 如果k2为None, 返回的是{'a': {'id': 'a', 'count': 1}, 'b': {'id': 'b', 'count'; 2}}
    """
    d = {}
    for a in arr:
        if k in a:
            d[a[k]] = a[k2] if k2 and k2 in a else (None if k2 and k2 not in a else a)
    return d


def start_2_end(start, end):
    """
    :param start: 开始时间
    :param end: 结束时间
    :return: "2016.05.01-2016.05.10"格式的字符串
    """
    d = 0 if start == PAY_ON_DATE else 1
    return str((start+datetime.timedelta(days=d)).strftime("%Y.%m.%d")) + '-' + str(end.strftime("%Y.%m.%d"))


def get_paid_users(start, end):
    """
    获得所有付费用户
    :param start: 开始时间
    :param end: 结束时间
    :return: 所有付过费的用户
    """
    pipeline = [
        {"$match": {"eventKey": "paymentSuccess", "serverTime": {"$gte": start, "$lt": end}, "user": {"$nin": inner_users}}},
        {"$group": {"_id": None, "users": {"$addToSet": "$user"}}}
    ]
    return list(events.aggregate(pipeline))[0]['users']


def get_date(d):
    """
    输入字符串时间, 获得datetime时间
    :param d: 字符串时间, 格式为 yyyy-mm-dd
    :return: 返回datetime时间
    """
    return datetime.datetime.strptime(d, "%Y-%m-%d") - datetime.timedelta(hours=8)


def dict_2_str(d):
    """
    输入一个dict, 返回"k1:v1, k2:v2, k3:v3"格式的字符串
    """
    s = ''
    for i in d:
        s = s+str(i)+':'+str(d[i]) + ','
    return s


def keys_2_dict(keys, ordered=False):
    """
    输入一个数组, 返回以该数组元素为key的dict
    :param keys: 数组
    :param ordered: 返回的dict是否为OrderedDict
    :return: 以数组元素为key的dict
    """
    if ordered:
        d = OrderedDict()
    else:
        d = dict()
    for k in keys:
        d[k] = 0
    return d


def arr_2_str(arr, sort=True):
    """
    统计输入数组中每个元素的个数, 并返回成字符串格式: {a:n1, b:n2,c:n3}
    :param arr: 需要统计的数组
    :param sort: 是否需要按照统计结果从多到少排序.
    :return: 返回统计结果
    """
    counter = Counter(arr).most_common() if sort else [(k, v) for k, v in Counter(arr).items()]
    return '{'+','.join([(str(c[0]) if type(c[0]) == int else c[0]) + ':'+str(c[1]) for c in counter])+'}'


def get_week_day(weekday):
    """
    获得今天之前的本周或上周星期几的日期, 比如想知道这个周六的日期(如果今天等于或大于周六, 否则得到的上周六的日期): get_last_day(6)
    :param weekday: 星期一:1, 星期二:2,... 星期天: 7.
    :return: 日期
    """
    idx = ((TODAY+datetime.timedelta(hours=8)).weekday() + 1) % 7
    if idx >= weekday:
        return TODAY - datetime.timedelta(idx-weekday)
    else:
        return TODAY - datetime.timedelta(7+idx-weekday)


def send_mail(send_from, send_to, subject, text, files=None, server=None, port=None, html=False, username=None, password=None):
    """
    :param send_from: 发件人
    :param send_to: 收件人, 可以为数组或字符串
    :param subject: 邮件标题
    :param text: 邮件正文, 可以为纯文本或html格式
    :param files: 附件, 数组
    :param server: 邮箱server, outlook: 'smtp.partner.outlook.cn'
    :param port: 邮箱port, outlook: 587
    :param html: 邮件内容是否为html
    :param username: 邮箱用户名
    :param password: 邮箱密码
    :return: None
    """

    if files is None:
        files = []

    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer['Subject'] = subject
    outer['To'] = send_to if isinstance(send_to, str) else COMMASPACE.join(send_to)
    outer['From'] = send_from
    outer['Date'] = formatdate(localtime=True)

    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'

    outer.attach(
            MIMEText(text, 'html' if html else 'plain'))

    # Add the attachments to the message
    for f in files:
        try:
            with open(f, 'rb') as fp:
                msg = MIMEBase('application', "octet-stream")
                msg.set_payload(fp.read())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(f))
            outer.attach(msg)
        except:
            print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
            raise

    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP(server, port) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(username, password)
            s.sendmail(send_from, send_to, composed)
            s.close()
        print("Sent email "+subject+" to "+outer['To'])
        print('------------------------------------------------------------')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise