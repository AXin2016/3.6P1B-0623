# -*- coding: utf-8 -*-

from config.util import *

endTime = TODAY
startTime = endTime - datetime.timedelta(weeks=1)

'''
漏斗1A 2A, platform: app
'''
funnel_steps_1 = [
    # 1A
    [
        {"name":"进入我的页面","key":"enterUserCenter","query":{"platform":"app"}},
        {"name":"点击新建洋葱圈","key":"clickUCCreateGroup","query":{"platform":"app"}},
        {"name":"创建成功","key":"createGroupSuccess","query":{"platform":"backend"}}
    ],

    # 2A
    [
        {"name":"进入我的页面","key":"enterUserCenter","query":{"platform":"app"}},
        {"name":"点击加入洋葱圈","key":"clickUCJoinGroup","query":{"platform":"app"}},
        {"name":"加入成功","key":"joinGroupSuccess","query":{"platform":"backend"}}
    ]
]

print("funnel 1A and 2A, platform: app")
for funnel_steps in funnel_steps_1:
    try:
        res_1 = sequential_funnel(startTime, endTime, funnel_steps, user='device')
        for each in res_1:
            print(each['name']+':'+ str(each['uv']))
        print('***')
    except:
        print("error in funnel: " + str(funnel_steps))
        print(sys.exc_info())
        raise

'''
漏斗1B 1C 2C, platform: web
'''
funnel_steps_2 = [
    # 1B
    [
        {"name":"进入学习主页","key":"enterChapter","query":{"platform":"web"}},
        {"name":"点击新建洋葱圈","key":"clickChapterCreateGroup","query":{"platform":"web"}},
        {"name":"创建成功","key":"createGroupSuccess","query":{"platform":"backend"}}
    ],

    # 1C
    [
        {"name":"进入个人中心","key":"enterUserCenter","query":{"platform":"web"}},
        {"name":"点击新建洋葱圈","key":"clickUCCreateGroup","query":{"platform":"web"}},
        {"name":"创建成功","key":"createGroupSuccess","query":{"platform":"backend"}}
    ],

    # 2B
    [
        {"name": "点击加入洋葱圈", "key": "clickUCJoinGroup", "query": {"platform": "web"}},
        {"name": "加入成功", "key": "joinGroupSuccess", "query": {"platform": "backend"}}
    ]
]

print("-------------------------------")
print("funnel 1B 1C and 2B, platform: web")
for funnel_steps in funnel_steps_2:
    try:
        res_2 = sequential_funnel(startTime, endTime, funnel_steps,user='user')
        for each in res_2:
            print(each['name']+':'+ str(each['uv']))
        print('***')
    except:
        print("error in funnel: " + str(funnel_steps))
        print(sys.exc_info())
        raise
