import requests
import json
import sys
from time import time, ctime, sleep
import random
import os
import MySQLdb


class dbMan(object):
    def __init__(self):
        self.db = MySQLdb.connect(host="pipe-writer", user="pipe", passwd="pipe123", db="pipe_tutor")

    def kylin_con(self):
        inter_headers = {'Authorization': 'Basic QURNSU46S1lMSU4='}
        kylin_connect = 'http://f04:9092/kylin/api/user/authentication'
        self.handle = requests.session()
        self.handle.headers.update({'Content-Type': 'application/json'})
        res = self.handle.post(kylin_connect, headers=inter_headers)

        print res.status_code

        if res.status_code == 200:
            return self.handle
        else:
            print "kylin_con_error"
            sys.exit()

    def kylin_query(self, sql, handle):
        url_query = 'http://f04:9092/kylin/api/query'
        json_str = '{"sql":"' + sql + '", "offset":0 ,"acceptPartial":false,"project":"Pipe"}'
        res = handle.post(url_query, data=json_str)
        return res

    def kylin_auto_query(self, sql):
        handle = self.kylin_con()
        res = self.kylin_query(sql, handle)
        while res.status_code == 401:
            sleep(0.1)
            handle = self.kylin_con()
            res = self.kylin_query(sql, handle)
        return res.json()['results']

    def get_cube_query_parm(self, cube_id):
        sql_method = '''select method from pipe_tutor.k_pipe_tutor_user_cube_info where id = %s ''' % (cube_id)
        sql_detail = '''select fsql from  pipe_tutor.k_pipe_tutor_user_cube_detail where cubeid = %s order by rank ''' % (
        cube_id)
        cursor = self.db.cursor()
        cursor.execute(sql_method)
        results = cursor.fetchall()
        print sql_method
        method = results[0][0]
        print method
        detail = []
        print sql_detail
        cursor.execute(sql_detail)
        print sql_detail
        results = cursor.fetchall()
        print results
        for row in results:
            detail.append(row[0])
        return_map = {}
        return_map['method'] = method
        return_map['detail'] = detail
        return return_map

    def save_query(self, cube_id, uid_set):
        random_id = random.randint(0, 10000)
        time_s = int(time())
        file_name = '''%s.txt''' % (str(random_id + time_s))
        file_dir = '''/user/kylin/user_cube/result'''

        res_file = open(file_name, "w")
        for uid in uid_set:
            res_file.write(uid + "\n")
        res_file.close()

        save_cmd = "hadoop fs -put %s %s" % (file_name, file_dir)
        os.system(save_cmd)

        clean_cmd = "rm %s" % (file_name)
        os.system(clean_cmd)

        return_sql = "update pipe_tutor.k_pipe_tutor_user_cube_info set path='%s',status='done' where id=%s " % (
        file_name, cube_id)
        cursor = self.db.cursor()
        cursor.execute(return_sql)
        self.db.commit()
        self.db.close()
