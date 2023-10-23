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


def get_type(v: object):
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

    def get_sql_type(self, field_type: str, field_format: str = '') -> str:
        if field_type not in self.FIELD_TYPES.keys():
            raise TypeError(f"No translation for '{field_type}' available.")
            return

        tval = self.FIELD_TYPES[field_type]['sql_type']
        # tval = self.FIELD_TYPES[field_type]
        if 'format' in tval:
            tval = tval.replace('format', field_format)

        return tval

    def make_python_type(self, value: object, python_type: str) -> object:
        match python_type:
            case 'integer':
                return int(value)
            case 'float':
                return float(value)
            case 'string':
                return str(value)
            case 'blob':
                return str(value)
            case 'boolean':
                if value.lower() in ['y', 'yes', 'true', '1']:
                    return 1
                return 0
            case _:
                return None

    def get_field(self, field_name: str, field_type: str = '', field_format: str = '',
                  field_modifier: str = None) -> str:
        ftype = ''
        if field_type:
            ftype = self.get_sql_type(field_type, field_format)

        fmod = ''
        if field_modifier is not None:
            fmod = self.PKEY

        return "'{field_name}' {ftype} {fmod}" \
            .replace('{field_name}', field_name) \
            .replace('{ftype}', ftype) \
            .replace('{fmod}', fmod) \
            .strip()

    def make_create_db_stmt(self, database_name: str, end: str = ';') -> str:
        stmt = self.STMTS['create_db']
        return stmt \
            .replace('{database_name}', database_name) \
            .replace('{end}', end) \
            .strip()

    def make_drop_db_stmt(self) -> None:
        raise NotImplementedError(f"Not implemented.")

    def make_create_table_stmt(self, table_name: str, field_defs: list, end: str = ';') -> str:
        stmt = self.STMTS['create_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{field_defs}', field_defs) \
            .replace('{end}', end) \
            .strip()

    def make_insert_stmt(self, table_name: str, field_defs: list, end: str = ';') -> str:
        stmt = self.STMTS['insert_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{field_list}', "', '".join(field_list)) \
            .replace('{value_list}', "', '".join(value_list)) \
            .replace('{end}', end) \
            .strip()

    def make_create_index_stmt(self, index_field: str, table_name: str, field_list: list = None,
                               index_prefix: str = "idx_", end: str = ';') -> str:
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

    def make_drop_table_stmt(self, table_name: str, end: str = ';') -> str:
        stmt = self.STMTS['drop_table']
        return stmt \
            .replace('{table_name}', table_name) \
            .replace('{end}', end) \
            .strip()

    def _make_db(cls, database_name: str = None) -> None:
        raise NotImplementedError(f"Not implemented.")


class SQLite3(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': {'sql_type': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                   'python_type': 'integer'},

        # core types
        'null': {'sql_type': 'NULL',
                 'python_type': 'None'},
        'integer': {'sql_type': 'INTEGER',
                    'python_type': 'integer'},
        'float': {'sql_type': 'FLOAT',
                  'python_type': 'float'},
        'text': {'sql_type': 'TEXT',
                 'python_type': 'string'},
        'blob': {'sql_type': 'BLOB',
                 'python_type': 'string'},

        # useful types
        'boolean': {'sql_type': 'INTEGER',
                    'python_type': 'integer'},
        'date': {'sql_type': 'TEXT',
                 'python_type': 'string'},
        'time': {'sql_type': 'TEXT',
                 'python_type': 'string'},
        'datetime': {'sql_type': 'TEXT',
                     'python_type': 'string'},
        'timestamp': {'sql_type': 'TEXT',
                      'python_type': 'string'},

        # other types
        'varchar': {'sql_type': 'TEXT',
                    'python_type': 'string'},
        'decimal': {'sql_type': 'TEXT',
                    'python_type': 'string'},
        'money': {'sql_type': 'TEXT',
                  'python_type': 'string'},
        'json': {'sql_type': 'TEXT',
                 'python_type': 'string'},
    }

    DEFAULT_TYPE = {'sql_type': 'TEXT',
                    'python_type': 'string'}

    STMTS = {
        'create_db': None,
        'create_index': "CREATE INDEX IF NOT EXISTS '{index_prefix}{index_name}' ON '{table_name}' ('{field_list}'){end}",
        'create_table': "CREATE TABLE IF NOT EXISTS '{table_name}' ({field_defs}){end}",
        'insert_table': "INSERT INTO '{table_name}' VALUES({values_list}){end}",
        'drop_table': "DROP TABLE IF EXISTS '{table_name}'{end}",
    }

    def __init__(self):
        self.STMTS['make_db'] = self.make_db

    def make_db(cls, database_name) -> None:
        raise NotImplementedError(f"Not implemented yet")
        return False


class MySQL(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': {'sql_type': 'SERIAL',
                   'python_type': 'integer'},

        # core types
        'null': {'sql_type': 'NULL',
                 'python_type': 'None'},
        'integer': {'sql_type': 'INTEGER',
                    'python_type': 'integer'},
        'float': {'sql_type': 'REAL',
                  'python_type': 'float'},
        'text': {'sql_type': 'TEXT',
                 'python_type': 'string'},
        'blob': {'sql_type': 'BLOB',
                 'python_type': 'string'},

        # useful types
        'boolean': {'sql_type': 'BOOLEAN',
                    'python_type': 'integer'},
        'date': {'sql_type': 'DATE',
                 'python_type': 'string'},
        'time': {'sql_type': 'TIME',
                 'python_type': 'string'},
        'datetime': {'sql_type': 'DATETIME',
                     'python_type': 'string'},
        'timestamp': {'sql_type': 'TIMESTAMP',
                      'python_type': 'string'},

        # other types
        'varchar': {'sql_type': 'VARCHAR(format)',
                    'python_type': 'string'},
        'decimal': {'sql_type': 'DECIMAL(format)',
                    'python_type': 'string'},
        'money': {'sql_type': 'DECIMAL[20,2]',
                  'python_type': 'float'},
        'json': {'sql_type': 'JSON',
                 'python_type': 'string'},
    }

    DEFAULT_TYPE = {'sql_type': 'VARCHAR(255)',
                    'python_type': 'string'}

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

    def get_sql_type(self, field_type: str, field_format: str = '') -> str:
        if field_type == 'money' and 0 == len(field_format):
            field_format = '12.2'
        return super().get_sql_type(field_type, field_format)


class PostgreSQL(Base):
    FIELD_TYPES = {
        # primary key type
        'serial': {'sql_type': 'SERIAL',
                   'python_type': 'integer'},

        # core types
        'null': {'sql_type': 'NULL',
                 'python_type': 'None'},
        'integer': {'sql_type': 'INTEGER',
                    'python_type': 'integer'},
        'float': {'sql_type': 'FLOAT',
                  'python_type': 'float'},
        'text': {'sql_type': 'TEXT',
                 'python_type': 'string'},
        'blob': {'sql_type': 'BYTEA',
                 'python_type': 'string'},

        # useful types
        'boolean': {'sql_type': 'BOOLEAN',
                    'python_type': 'integer'},
        'date': {'sql_type': 'DATE',
                 'python_type': 'string'},
        'time': {'sql_type': 'TIME',
                 'python_type': 'string'},
        'datetime': {'sql_type': 'TIMESTAMP',
                     'python_type': 'string'},
        'timestamp': {'sql_type': 'TIMESTAMP',
                      'python_type': 'string'},

        # other types
        'varchar': {'sql_type': 'VARCHAR(format)',
                    'python_type': 'string'},
        'decimal': {'sql_type': 'DECIMAL(format)',
                    'python_type': 'string'},
        'money': {'sql_type': 'MONEY',
                  'python_type': 'float'},
        'json': {'sql_type': 'JSON',
                 'python_type': 'string'},
    }

    DEFAULT_TYPE = {'sql_type': 'TEXT',
                    'python_type': 'string'}

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

    def make_db(cls, database_name: str) -> str:
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
    print('SQLITE: ' + s.get_sql_type('integer'))
    print('MYSQL: ' + m.get_sql_type('integer'))
    print('POSTGRES: ' + p.get_sql_type('integer'))

    print('\nDecimal Types (formatted)\n' + dash())
    print('SQLITE: ' + s.get_sql_type('decimal', '12,2'))
    print('MYSQL: ' + m.get_sql_type('decimal', '12,2'))
    print('POSTGRES: ' + p.get_sql_type('decimal', '12,2'))

    print('\nMoney Types (pre-fornatted)\n' + dash())
    print('SQLITE: ' + s.get_sql_type('money'))
    print('MYSQL: ' + m.get_sql_type('money'))
    print('POSTGRES: ' + p.get_sql_type('money'))

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
