from dbMan import dbMan
import util
import sys
from threadingMan import threadingMan


class userCube(object):
    def __init__(self):
        self.kylin = dbMan()

    def get_cube_detail(self, cube_id):
        sql_map = self.kylin.get_cube_query_parm(cube_id)
        return sql_map

    def threading_run(self, sql_list):
        threads = []
        res = []
        nSql = range(len(sql_list))
        for sql in sql_list:
            t = threadingMan(self.kylin.kylin_auto_query, (sql,))
            threads.append(t)

        for i in nSql:
            threads[i].start()

        for i in nSql:
            threads[i].join()
            res.append(threads[i].get_result())

        res_list = util.uid_list_to_set(res)
        return res_list

    def run(self, cube_id):
        sql_map = self.get_cube_detail(cube_id)
        res_list = self.threading_run(sql_map['detail'])
        uid_set = util.uid_arithmetic(res_list, sql_map['method'])
        self.kylin.save_query(cube_id, uid_set)


def main():
    if len(sys.argv) < 2:
        print "Usage: userCube.py {{cube_id}}"
        return
    user_cube = userCube()
    user_cube.run(sys.argv[1])


if __name__ == '__main__':
    main()
