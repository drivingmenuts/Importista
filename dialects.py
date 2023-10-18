#!/usr/bin/env python3

import inspect


def __dir__():
    return [
        'get_dialect',
        'get_type',
        'Base',
        'SQLite3',
        'MySQL',
        'PostgreSQL'
    ]


def get_dialect(d):
    match d:
        case 'SQLite3':
            return SQLite3
        case 'MySQL':
            return MySQL
        case 'PostgreSQL':
            return PostgreSQL
        case _:
            raise NotImplementedError(f"{d} not an implemented dialect.")
            exit()


def get_type(v):
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    return "str"


class Base:
    FIELD_TYPES = None
    DEFAULT_TYPE = None
    STMTS = None

    def __init__(self):
        pass

    def get_type(self, field_type, field_format=''):
        if field_type not in self.FIELD_TYPES.keys():
            raise TypeError(f"No translation for '{field_type}' available.")
            return

        tval = self.FIELD_TYPES[field_type]
        if 'format' in tval:
            tval = tval.replace('format', field_format)

        return tval

    def get_field(self, field_name, field_type='', field_format='', field_modifier=None):
        ftype = ''
        if field_type:
            ftype = self.get_type(field_type, field_format)

        fmod = ''
        if field_modifier is not None:
            fmod = self.PKEY

        return "'{field_name}' {ftype} {fmod}" \
            .replace('{field_name}', field_name) \
            .replace('{ftype}', ftype) \
            .replace('{fmod}', fmod) \
            .strip()

    def make_create_db_stmt(self, database_name, end=';'):
        stmt = self.STMTS['create_db']
        return stmt \
            .replace('{database_name}', database_name) \
            .replace('{end}', end) \
            .strip()

    def make_drop_db_stmt(self):
        raise NotImplementedError(f"Not implemented.")

    def make_create_table_stmt(self, table_name, field_defs, end=';'):
        stmt = self.STMTS['create_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{field_defs}', field_defs) \
            .replace('{end}', end) \
            .strip()

    def make_insert_stmt(self, table_name, field_list, value_list, end=';'):
        stmt = self.STMTS['insert_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{field_list}', "', '".join(field_list)) \
            .replace('{value_list}', "', '".join(value_list)) \
            .replace('{end}', end) \
            .strip()

    def make_create_index_stmt(self, index_field, table_name, field_list=None, index_prefix="idx_", end=';'):
        stmt = self.STMTS['create_index']
        if field_list is None:
            field_list = [index_field]
        return stmt \
            .replace('{index_prefix}', index_prefix) \
            .replace('{index_name}', index_field) \
            .replace('{table_name}', table_name) \
            .replace('{field_list}', "', '".join(field_list)) \
            .replace('{end}', end) \
            .strip()

    def make_drop_table_stmt(self, table_name, end=';'):
        stmt = self.STMTS['drop_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{end}', end) \
            .strip()

    def _make_db(cls, database_name=None):
        raise NotImplementedError(f"Not implemented.")


class SQLite3(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': 'INTEGER PRIMARY KEY AUTOINCREMENT',

        # core types
        'null': 'NULL',
        'integer': 'INTEGER',
        'float': 'FLOAT',
        'text': 'TEXT',
        'blob': 'BLOB',

        # useful types
        'boolean': 'INTEGER',
        'date': 'TEXT',
        'time': 'TEXT',
        'datetime': 'TEXT',
        'timestamp': 'TEXT',

        # other types
        'varchar': 'TEXT',
        'decimal': 'TEXT',
        'money': 'TEXT',
        'json': 'TEXT'
    }
    DEFAULT_TYPE = 'TEXT'
    STMTS = {
        'create_db': None,
        'create_index': "CREATE INDEX IF NOT EXISTS '{index_prefix}{index_name}' ON '{table_name}' ('{field_list}'){end}",
        'create_table': "CREATE TABLE IF NOT EXISTS '{table_name}' ({field_list}) VALUES ({value_list}){end}",
        'insert_table': "INSERT INTO '{table_name}' VALUES({values_list}){end}",
        'drop_table': "DROP TABLE IF EXISTS '{table_name}'{end}",
    }

    def __init__(self):
        self.STMTS['make_db'] = self.make_db

    def make_db(cls, database_name):
        raise NotImplementedError(f"Not implemented yet")
        return False


class MySQL(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': 'SERIAL',

        # shared types
        'null': 'NULL',
        'integer': 'INTEGER',
        'float': 'REAL',
        'text': 'TEXT',
        'blob': 'BLOB',

        # useful types
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'DATETIME',
        'timestamp': 'TIMESTAMP',

        # other types
        'varchar': 'VARCHAR(format)',
        'decimal': 'DECIMAL(format)',
        'money': 'DECIMAL[20,2]',
        'json': 'JSON'
    }
    DEFAULT_TYPE = 'VARCHAR(255)'
    STMTS = {
        'create_db': "CREATE DATABASE IF NOT EXISTS '{database_name}'{end} USE '{database_name}'{end}",
        'create_index': "CREATE INDEX IF NOT EXISTS '{index_prefix}{index_name}' ON '{table_name}' ('{field_list}'){end}",
        'create_table': "CREATE TABLE IF NOT EXISTS '{table_name}' ({field_defs}){end}",
        'insert_table': "INSERT INTO '{table_name}' VALUES({values_list}){end}",
        'drop_table': "DROP TABLE IF EXISTS '{table_name}'{end}",
    }

    def __init__(self):
        class_name = self.__class__.__name__
        print(f"Warning: {class_name} dialect has not yet been tested!")

    def get_type(self, field_type, field_format=''):
        if field_type == 'money' and 0 == len(field_format):
            field_format = '12.2'
        return super().get_type(field_type, field_format)


class PostgreSQL(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': 'SERIAL',

        # shared types
        'null': 'NULL',
        'integer': 'INTEGER',
        'float': 'FLOAT',
        'text': 'TEXT',
        'blob': 'BYTEA',

        # useful types
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',

        # other types
        'varchar': 'VARCHAR(format)',
        'decimal': 'DECIMAL(format)',
        'money': 'MONEY',
        'json': 'JSON'
    }
    DEFAULT_TYPE = 'TEXT'
    STMTS = {
        'make_db': None,
        'create_index': "CREATE INDEX IF NOT EXISTS '{index_prefix}{index_name}' ON '{table_name}' ('{field_list}'){end}",
        'create_table': "CREATE TABLE IF NOT EXISTS '{table_name}' ({field_defs}){end}",
        'insert_table': "INSERT INTO '{table_name}' VALUES({values_list}){end}",
        'drop_table': "DROP TABLE IF EXISTS '{table_name}'{end}",
    }

    def __init__(self):
        class_name = self.__class__.__name__
        print(f"Warning: {class_name} dialect has not yet been tested!")
        self.STMTS['make_db'] = self.make_db

    def make_db(cls, database_name):
        raise NotImplementedError(f"Not implemented yet")
        return False


if __name__ == '__main__':
    from pprint import pprint as pp

    TEST_DATABASE = 'testbase'
    TEST_TABLE = 'test_table'


    def dash(dash_width=20):
        return '-' * dash_width


    s = SQLite3()
    m = MySQL()
    p = PostgreSQL()

    print('\nInteger Types\n' + dash())
    print('SQLITE: ' + s.get_type('integer'))
    print('MYSQL: ' + m.get_type('integer'))
    print('POSTGRES: ' + p.get_type('integer'))

    print('\nDecimal Types (formatted)\n' + dash())
    print('SQLITE: ' + s.get_type('decimal', '12,2'))
    print('MYSQL: ' + m.get_type('decimal', '12,2'))
    print('POSTGRES: ' + p.get_type('decimal', '12,2'))

    print('\nMoney Types (pre-fornatted)\n' + dash())
    print('SQLITE: ' + s.get_type('money'))
    print('MYSQL: ' + m.get_type('money'))
    print('POSTGRES: ' + p.get_type('money'))

    print('\nCreating databases\n' + dash())
    print('MYSQL: ' + m.make_create_db_stmt(TEST_DATABASE))

    print('\nCreating tables\n' + dash())
    fields = {'person_id': 'serial', 'employee_type': 'integer', 'money': 'money', 'food': 'text'}
    values = ['', 1, 12.00, 'Hamburger']

    sqlite_defs = ','.join([s.get_field(fn, fields[fn]) for fn in fields.keys()])
    print('SQLITE: \t' + s.make_create_table_stmt(TEST_TABLE, sqlite_defs))
    print('\t\t' + s.make_create_index_stmt('employee_type', TEST_TABLE), '\n\n')

    mysql_defs = ','.join([m.get_field(fn, fields[fn]) for fn in fields.keys()])
    print('MYSQL: \t\t' + m.make_create_table_stmt(TEST_TABLE, mysql_defs))
    print('\t\t' + m.make_create_index_stmt('etype_pid', TEST_TABLE, ['employee_type', 'person_id']), '\n\n')

    postgres_defs = ','.join([m.get_field(fn, fields[fn]) for fn in fields.keys()])
    print('POSTGRES: \t' + m.make_create_table_stmt(TEST_TABLE, postgres_defs))
    print('\t\t' + p.make_create_index_stmt('etype_pid', TEST_TABLE, ['employee_type', 'person_id'], 'index_'), '\n\n')

    print('\nDropping tables\n' + dash())
    print('SQLITE: ' + s.make_drop_table_stmt(TEST_TABLE))
    print('MYSQL: ' + m.make_drop_table_stmt(TEST_TABLE))
    print('POSTGRES: ' + p.make_drop_table_stmt(TEST_TABLE))

    print('\nDone.')
