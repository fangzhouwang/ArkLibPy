#!/usr/bin/env python3


import mysql.connector

__version__ = '1.0.0'


class ArkDBMySQL:
    def __init__(self, **kwargs):
        """
            db = ArkDBMySQL( [ table = ''] [, db_config_file = ''] )
            constructor method
                table is for CRUD methods
                filename is for connecting to the database file
        """
        db_config_file = kwargs.get('db_config_file', '')
        if db_config_file:
            with open(db_config_file) as config:
                self.host_ = config.readline().rstrip()
                self.user_ = config.readline().rstrip()
                self.password_ = config.readline().rstrip()
                self.schema_ = config.readline().rstrip()
                self.port_ = int(config.readline().rstrip())
        else:
            self.host_ = kwargs.get('host')
            self.user_ = kwargs.get('user')
            self.password_ = kwargs.get('password')
            self.schema_ = kwargs.get('schema')
            self.port_ = kwargs.get('port', 3306)   # default MySQL port is 3306

        self.table_ = ''
        self.con_ = mysql.connector.connect(
            user=self.user_,
            password=self.password_,
            host=self.host_,
            database=self.schema_,
        )
        self.cur_ = self.con_.cursor(dictionary=True, buffered=True)
        self.err_ = None

    def __del__(self):
        self.con_.close()

    def set_table(self, table_name):
        self.table_ = table_name

    def get_table(self):
        return self.table_

    def set_auto_inc(self, inc):
        cur_inc = self.get_auto_inc()
        if cur_inc >= inc:
            print(f'Error: Current value {cur_inc} is larger than {inc}')
            return False
        self.run_sql(f'ALTER TABLE {self.table_} AUTO_INCREMENT=%s', [inc])
        return True

    def get_auto_inc(self):
        return self.get_query_value('AUTO_INCREMENT', f"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{self.schema_}' AND TABLE_NAME='{self.table_}'")

    def run_sql_nocommit(self, sql, params=()):
        self.err_ = None
        try:
            self.cur_.execute(sql, params)
        except mysql.connector.Error as err:
            self.err_ = err
            print("DB error: {}".format(err))

    def commit(self):
        self.con_.commit()

    def get_error(self):
        return self.err_

    def run_sql(self, sql, params=()):
        self.run_sql_nocommit(sql, params)
        self.commit()

    def run_query_get_all_row(self, query, params=()):
        self.run_sql_nocommit(query, params)
        return self.cur_.fetchall()

    def run_query(self, query, params=()):
        self.run_sql_nocommit(query, params)
        row = self.cur_.fetchone()
        while row is not None:
            yield row
            row = self.cur_.fetchone()

    def get_query_row(self, query, params=()):
        self.run_sql_nocommit(query, params)
        return self.cur_.fetchone()

    def get_query_value(self, val_name, query, params=()):
        row = self.get_query_row(query, params)
        if row is None or val_name not in row:
            return None
        return row[val_name]

    def insert_nocommit(self, rec):
        klist = sorted(rec.keys())
        values = [rec[v] for v in klist]
        query = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self.table_,
            ', '.join(klist),
            ', '.join(['%s'] * len(values))
        )
        self.run_sql_nocommit(query, values)
        return self.cur_.lastrowid

    def insert(self, rec):
        lastrowid = self.insert_nocommit(rec)
        self.commit()
        return lastrowid

    def update_nocommit(self, recid, recid_label, rec):
        klist = sorted(rec.keys())
        values = [rec[v] for v in klist]

        # do not update id
        for i, k in enumerate(klist):
            if k == recid_label:
                del klist[i]
                del values[i]

        query = 'UPDATE {} SET {} WHERE {} = %s'.format(
            self.table_,
            ', '.join(map(lambda s: '{} = %s'.format(s), klist)),
            recid_label
        )
        self.run_sql_nocommit(query, values + [recid])

    def update(self, recid, recid_label, rec):
        self.update_nocommit(recid, recid_label, rec)
        self.commit()

    def delete_nocommit(self, recid, recid_label):
        query = f'DELETE FROM {self.table_} WHERE {recid_label} = %s'
        self.run_sql_nocommit(query, [recid])

    def delete(self, recid, recid_label):
        self.delete_nocommit(recid, recid_label)
        self.commit()

    def optimize(self, table=None):
        if not table:
            table = self.table_
        self.run_sql(f'optimize table {table}')


    def is_table_exist(self, table):
        res = self.run_query_get_all_row(f"SHOW TABLES LIKE '{table}'")
        return len(res) != 0

    def add_index(self, item, table=None):
        if not table:
            table = self.table_
        indexes = self.run_query_get_all_row(f'SHOW INDEX FROM {table}')
        for row in indexes:
            if row['Column_name'] == item:
                print(f'Index {item} already exists in table {table}')
                return False
        self.run_sql(f'ALTER TABLE {table} ADD INDEX ({item})')
        print(f'Added index {item} for table {table}')
        return True

    def remove_index(self, item, table=None):
        if not table:
            table = self.table_
        indexes = self.run_query_get_all_row(f'SHOW INDEX FROM {table}')
        for row in indexes:
            if row['Column_name'] == item:
                break
        else:
            print(f'Index {item} does not exist in table {table}')
            return False
        self.run_sql(f'DROP INDEX `{item}` ON {table}')
        print(f'Removed index {item} for table {table}')
        return True

    def get_table_disk_size(self, table=None):
        if not table:
            table = self.table_
        data_size = self.get_query_value('size_in_mb', f'SELECT table_name AS `Table`, round(((data_length) / 1024 / 1024), 2) AS `size_in_mb` FROM information_schema.TABLES WHERE table_schema = "{self.schema_}" AND table_name = "{table}"')
        index_size = self.get_query_value('size_in_mb', f'SELECT table_name AS `Table`, round(((index_length) / 1024 / 1024), 2) AS `size_in_mb` FROM information_schema.TABLES WHERE table_schema = "{self.schema_}" AND table_name = "{table}"')
        return data_size, index_size

    def create_table(self, table_desc_dict, force=False):
        if self.is_table_exist(table_desc_dict['table_name']):
            if not force:
                return False
            self.run_sql(f"DROP TABLE {table_desc_dict['table_name']}")
        columns_desc_list = [f"`{item['name']}` {item['type']} {item['property']}"
                            for item in table_desc_dict['table_columns']]
        query = f"CREATE TABLE `{table_desc_dict['table_name']}` ("
        query += ', '.join(columns_desc_list)
        query += ', PRIMARY KEY ('
        query += ', '.join([f'`{col}`' for col in table_desc_dict['table_pks']])
        query += '))'
        query += 'ENGINE = InnoDB'
        self.run_sql(query)
        return True

