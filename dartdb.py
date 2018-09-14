import pymysql

db_config = {
    'host': '',
    'port': 3306,
    'user': '',
    'password': '',
    'db': '',
    'charset': 'utf8'
}


class DartDb:
    def __init__(self):
        self._connect_db()

    def execute(self, sql):
        return self.curs.execute(sql)

    def close(self):
        self.conn.commit()
        self.conn.close()

    def get_cursor(self):
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)

    def close_cursor(self):
        self.curs.close()

    def _connect_db(self):
        self.conn = pymysql.connect(**db_config)
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)

    def _debug_clear_table(self, table_name):
        sql = 'TRUNCATE TABLE `{}`'.format(table_name)
        self.curs.execute(sql)

    def _debug_print_all_rows(self):
        sql = 'SELECT * FROM `naver_news`'
        self.curs.execute(sql)
        rows = self.curs.fetchall()
        for row in rows:
            print(row['title'])