import unittest
from ArkDBMySQL import ArkDBMySQL


class ArkDBMySQLTestCase(unittest.TestCase):
    def setUp(self):
        self.db_ = ArkDBMySQL(db_config_file='/Users/Ark/.db_configs/db_config_local_tester.txt')
        self.table_ = "test_table"

    def test_config_file(self):
        DB_HOST = "localhost"
        DB_USER = "tester"
        DB_PASSWORD = "tester"
        DB_SCHEMA = "gtest"
        DB_PORT = 3306

        self.db_ = ArkDBMySQL(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, schema=DB_SCHEMA, port=DB_PORT)
        self.table_ = "test_table"
        self.db_.run_sql(f'DROP TABLE IF EXISTS {self.table_}')
        self.db_.run_sql(f'CREATE TABLE {self.table_} (str_col VARCHAR(20), int_col INT PRIMARY KEY)')

    def test_connection(self):
        self.db_.run_sql(f'DROP TABLE IF EXISTS {self.table_}')
        self.db_.run_sql(f'CREATE TABLE {self.table_} (str_col VARCHAR(20), int_col INT PRIMARY KEY)')

        self.db_.set_table(self.table_)
        rowid = self.db_.insert({"int_col" : 1, "str_col" : "test"})
        self.assertEqual(rowid, 0)
        self.db_.insert({"int_col" : 2, "str_col" : "test"})
        self.db_.update(2, 'int_col', {"int_col" : 5, "str_col" : "test"})
        self.db_.delete(1, 'int_col')
        temp = self.db_.get_query_value('int_col', 'SELECT int_col FROM test_table')
        self.assertEqual(temp, 2)

if __name__ == '__main__':
    unittest.main()
