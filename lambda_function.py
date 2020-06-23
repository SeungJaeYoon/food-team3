import os
import json
import pymysql
import boto3
import csv
import base64
import copy
import requests
import operator

def user_id_post(event, context):
    id_ = int(event['body']['id'])
    mode = int(event['body']['mode'])
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    table_name = 'food_data_new%i'%mode
    key_id = 'AKIAXXPRRAWGIVPKHXSN'
    secret_access_key = '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'
    bucket_name = 'team3-user'
    s3 = boto3.client('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_access_key,
                      region_name='ap-northeast-2')
    cur.execute("select exists(select * from user_table%i where id=%s) as bool;" % (mode,id_))
    result = cur.fetchone()
    existance = result['bool']
    if existance == 0:
        with open('/tmp/event.csv', 'w', encoding='utf-8') as f:
            wr = csv.writer(f)
            wr.writerow(["EventId,EventName,EventTime,FoodID"])
        s3.upload_file('/tmp/event.csv', bucket_name, '%s/event.csv' % (id_))
        os.remove('/tmp/event.csv')
        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": {'id': id_}
        }
    else:
        return {
            "isBase64Encoded": False,
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": {}
        }


def user_basic_info_post(event, context):
    # TODO implement
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    id_ = int(event['body']['id'])
    gender = int(event['body']['gender'])
    age = int(event['body']['age'])
    height = int(event['body']['height'])
    weight = int(event['body']['weight'])
    mode = int(event['body']['mode'])
    cur.execute("insert into user_table%i(id,gender,age,height,weight)\nvalues(%i,%i,%i,%i,%i)" % (mode,
    id_, gender, age, height, weight))
    cur.execute("insert into user_rating%i(id)\nvalues(%i)" % (mode,id_))
    cur.execute("insert into food_select%i(id)\nvalues(%i)" % (mode,id_))
    connection.commit()
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": {}
    }


def user_rating_update(event, context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    mode = int(event['body']['mode'])
    print('mode')
    print(mode)
    rating_dat_dic = event['body']
    print("rating_dat_dic")
    print(rating_dat_dic)
    user_id = int(rating_dat_dic['user'])
    print("user_id")
    print(user_id)
    rating_dic = rating_dat_dic['result']
    print("rating_dic")
    print(rating_dic)
    food_id = int(rating_dic['id'])
    print("food_id")
    print(food_id)
    food_rate_score = int(rating_dic['rating'])
    print("")
    cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i" % (mode,food_id, food_rate_score, user_id))
    connection.commit()
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": {}
    }


def user_basic_info_get(event, context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    mode = int(event['body']['mode'])
    cur.execute("select * from user_table%i NATURAL JOIN user_rating%i"%(mode,mode))
    result = cur.fetchall()
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": result
    }


def user_each_info_get(event, context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    uid = int(event['body']['id'])
    mode = int(event['body']['mode'])
    cur.execute("select * from user_table%i NATURAL JOIN user_rating%i where id = %i" % (mode,mode,uid))
    result = cur.fetchall()
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": result
    }


def food_recommendation_random_get(event, context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    key_id = 'AKIAXXPRRAWGIVPKHXSN'
    secret_access_key = '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'
    bucket_name = 'team3-foodimage'
    s3 = boto3.resource('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_access_key,
                        region_name='ap-northeast-2')
    userid = event['body']['id']
    mode = int(event['body']['mode'])
    user_info = user_each_info_get(event, context)['body']
    new_dict = user_info[0]
    keys = list(new_dict.keys())
    final = {}
    final.update({'id':new_dict['id']})
    final.update({'height': new_dict['height']})
    final.update({'weight': new_dict['weight']})
    if new_dict['gender'] == 1:
        final.update({'gender': 'male'})
    else:
        final.update({'gender': 'female'})
    final.update({'age': new_dict['age']})

    del new_dict['id']
    del new_dict['height']
    del new_dict['weight']
    del new_dict['gender']
    del new_dict['age']

    keys.remove('id')
    keys.remove('height')
    keys.remove('weight')
    keys.remove('gender')
    keys.remove('age')

    for i, key in enumerate(keys):
        if new_dict[key] is None:
            new_dict[key] = 1
    
        final.update({'rate_' + str(i): new_dict[key]})

    r = requests.post(
        'http://52.79.172.42:5000/predict',
        headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        },
        json=final
    )
    answer = json.loads(r.json()['user_0'])
    sort_orders = sorted(answer.items(), key=lambda x: x[1], reverse=True)

    food_num = sort_orders[0][0].split('_')[1]
    foodid, rate = list(new_dict.items())[int(food_num)]
    for data in sort_orders:
        food_num = data[0].split('_')[1]
        foodid, rate = list(new_dict.items())[int(food_num)]
        if rate == 1:
            print('found one')
            print(rate)
            print(foodid)
            break
    foodid = int(foodid)
    print(foodid,userid)
    cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i" % (mode,foodid, 2, userid))
    connection.commit()
    print(foodid)
    cur.execute("select * from food_data_new%i where id = %i"%(mode,foodid))
    result = cur.fetchone()
    for i in result:
        if 'img' in i:
            if result[i] == None:
                pass
            else:
                if 'main_img' in i:
                    path = result[i]
                    bucket_name = path.split(',')[0]
                    object_name = path.split(',')[1]
                    obj = s3.Object(bucket_name, object_name)
                    body = obj.get()['Body'].read()
                    result[i] = base64.b64encode(body).decode("utf-8")
                else:
                    result[i] = None
    response = {
        "isBase64Encoded": True,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": result
    }
    return response


def food_data_get(event, context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                                 user="team_three",
                                 password="berthurivts4mdv2ap",
                                 port=3306,
                                 db='team_three',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    key_id = 'AKIAXXPRRAWGIVPKHXSN'
    secret_access_key = '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'
    bucket_name = 'team3-foodimage'
    s3 = boto3.resource('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_access_key,
                        region_name='ap-northeast-2')
    id_ = int(event['body']['id'])
    mode = int(event['body']['mode'])
    cur.execute("select * from food_data_new%i where id=%i" % (mode,id_))
    result = cur.fetchall()[0]
    new_dict = copy.deepcopy(result)
    for i in result:
        if 'img' in i:
            if result[i] == None:
                pass
            else:
                path = result[i]
                bucket_name = path.split(',')[0]
                object_name = path.split(',')[1]
                obj = s3.Object(bucket_name, object_name)
                body = obj.get()['Body'].read()
                new_dict[i] = base64.b64encode(body).decode("utf-8")
    response = {
        "isBase64Encoded": True,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": new_dict
    }
    return response


def user_event_update(event,context):
    connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                             user="team_three",
                             password="berthurivts4mdv2ap",
                             port = 3306,
                             db='team_three',
                             cursorclass=pymysql.cursors.DictCursor)
    cur= connection.cursor()
    key_id='AKIAXXPRRAWGIVPKHXSN'
    secret_access_key='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'
    bucket_name='team3-user'
    s3=boto3.client('s3',aws_access_key_id=key_id,aws_secret_access_key=secret_access_key,region_name='ap-northeast-2')
    event_dict=event['body']
    user_id=int(event_dict['user'])
    food_id=event_dict['foodId']
    time=event_dict['time']
    mode = int(event_dict['mode'])
    event_name=event_dict['eventName']
    #device=event_dict['device']
    #location=event_dict['location']
    #event_type=event_dict['event_type']
    if event_name=='itemClicked':
        food_id=int(food_id)
        cur.execute("select `%i` from food_select%i where id=%i"%(food_id,mode,user_id))
        result = cur.fetchone()
        select_num = result['%s'%food_id]
        if select_num==None:
            select_num=1
        else:
            select_num=int(select_num)
            select_num+=1
            cur.execute("UPDATE food_select%i SET `%i`=%i where id=%i"%(mode,food_id,select_num,user_id))
        cur.execute("select `%i` from user_rating%i where id=%i"%(food_id,mode,user_id))
        result = cur.fetchone()
        original_rating = result['%s'%food_id]
        if int(original_rating) < 3:
            cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i"%(mode,food_id,3,user_id))
    elif event_name=='10s':
        food_id=int(food_id)
        cur.execute("select `%i` from user_rating%i where id=%i"%(food_id,mode,user_id))
        result = cur.fetchone()
        original_rating = result['%s'%food_id]
        if int(original_rating) < 4:
            cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i"%(mode,food_id,4,user_id))
    elif event_name=='favourite':
        food_id=int(food_id)
        cur.execute("select `%i` from user_rating%i where id=%i"%(food_id,mode,user_id))
        result = cur.fetchone()
        original_rating = result['%s'%food_id]
        if int(original_rating) < 5:
            cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i"%(mode,food_id,5,user_id))
    elif event_name=='refresh':
        food_ids=food_id.split(',')
        food_ids.remove('')
        for food_id in food_ids:
            food_id=int(food_id)
            cur.execute("select `%i` from user_rating%i where id=%i"%(food_id,mode,user_id))
            result = cur.fetchone()
            original_rating = result['%s'%food_id]
            if int(original_rating) < 2:
                cur.execute("UPDATE user_rating%i SET `%i`=%i where id=%i"%(mode,food_id,2,user_id))

    s3.download_file(bucket_name, '%s/event.csv'%user_id , '/tmp/event.csv')
    total_line=[]
    with open('/tmp/event.csv','r',encoding='utf-8') as f:
        rdr=csv.reader(f)
        for line in rdr:
            total_line.append(line)
    try:
        index=int(total_line[-1][0])+1
    except:
        index=0
    with open('/tmp/event.csv','w',encoding='utf-8') as f:
        wr = csv.writer(f)
        for i in total_line:
            wr.writerow(i)
        wr.writerow([index,event_name,time,food_id])
    s3.upload_file('/tmp/event.csv',bucket_name,'%s/event.csv'%(user_id))
    os.remove('/tmp/event.csv')
    connection.commit()
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": {}
    }
