from dbMan import dbMan
import util
import sys
from threadingMan import threadingMan

kylin = dbMan()

cube_id = 0

sql1 = """ select distinct userid from temp.TUTOR_ORDER_KYLIN_TEST where episode_type='season' and source='10' limit 8 """
sql2 = """ select distinct userid from temp.TUTOR_ORDER_KYLIN_TEST where episode_type='lecture' and source='20' limit 10 """
sql3 = """ select distinct userid from temp.TUTOR_ORDER_KYLIN_TEST where episode_type='tutorial' and source='10' limit 10 """
sql4 = """ select distinct userid from temp.TUTOR_ORDER_KYLIN_TEST where episode_type='season' and source='30' limit 10 """
sql5 = """ select distinct userid from temp.TUTOR_ORDER_KYLIN_TEST where episode_type='season' and source='40' limit 10 """

sql_list = [sql1, sql2, sql3, sql4, sql5]

threads = []

res = []

nSql = range(len(sql_list))

for sql in sql_list:
    t = threadingMan(kylin.kylin_auto_query, (sql,))
    threads.append(t)

for i in nSql:
    threads[i].start()

for i in nSql:
    threads[i].join()
    res.append(threads[i].get_result())

res_list = util.uid_list_to_set(res)

arithmetic_method = '''(uid_list[0]-uid_list[1])|uid_list[2]|uid_list[3]|uid_list[4]'''
uid_set = util.uid_arithmetic(res_list, arithmetic_method)
# kylin.save_query(cube_id,uid_set)
print uid_set

# results = kylin.kylin_auto_query(sql1)

# handle = kylin.kylin_con()
# results = kylin.kylin_query(sql,handle)

# set_result = util.uid_list_to_set(results)

# print sys.getsizeof(set_result)
