import pymysql

db_config = {
    'host': '',
    'port': 0,
    'user': '',
    'password': '',
    'db': '',
    'charset': ''
}


class DartDb:
    def __init__(self, config=db_config):
        self._connect_db(config)

    def execute(self, sql):
        return self.curs.execute(sql)

    def close(self):
        self.conn.commit()
        self.conn.close()

    def get_cursor(self):
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)

    def close_cursor(self):
        self.curs.close()

    def _connect_db(self, config):
        self.conn = pymysql.connect(**config)
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)

    def _debug_clear_table(self, table_name):
        sql = 'TRUNCATE TABLE `{}`'.format(table_name)
        self.curs.execute(sql)

    def _debug_print_all_rows(self, table_name):
        sql = 'SELECT * FROM `{}`'.format(table_name)
        self.curs.execute(sql)
        rows = self.curs.fetchall()
        for row in rows:
            print(row['title'])
