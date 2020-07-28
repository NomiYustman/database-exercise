import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List
from db_api import DBField, SelectionCriteria, DB_ROOT, DBTable

from dataclasses_json import dataclass_json
from files_functions import create_json_file, insert_dict_to_json_file, read_json_file, delete_from_json_file


class TableExistError(Exception):
    pass


def create_table_file(db_table: DBTable) -> None:
    file = create_json_file(db_table.name + '.json')
    insert_dict_to_json_file(file, {})


def read_tables() -> dict:
    return read_json_file('tables.json')


@dataclass_json
@dataclass
class DataBase:
    def __init__(self):
        self.num = 0
        with open(DB_ROOT / 'tables.json', 'w') as tables_file:
            dictionary = {}
            json.dump(dictionary, tables_file)

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        dictionary = read_tables()
        if table_name in dictionary:
            raise TableExistError

        with open(DB_ROOT / 'tables.json', 'w+') as tables_file:
            db_table = DBTable(table_name, fields, key_field_name)
            create_table_file(db_table)

            dictionary[table_name] = {'fields': [field.name for field in fields], 'key field name': key_field_name}
            self.num += 1

            json.dump(dictionary, tables_file)

        return db_table

    def num_tables(self) -> int:
        return self.num

    def get_table(self, table_name: str) -> DBTable:
        dictionary = read_tables()
        return DBTable(table_name, dictionary[table_name]['fields'], dictionary[table_name]['key field name'])

    def delete_table(self, table_name: str) -> None:
        self.num -= 1
        os.remove(table_name + '.json')
        delete_from_json_file('tables.json', [table_name])

    def get_tables_names(self) -> List[Any]:
        dictionary = read_tables()
        return list(dictionary.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


db = DataBase()
db.create_table('library', [DBField('name', str), DBField('books', list), DBField('address', str)], 'name')
db.create_table('book', [DBField('id', int), DBField('name', str), DBField('writer', str), DBField('pages', int)], 'id')
print(db.num_tables())
print(db.get_table('library'))
print(db.get_tables_names())
library = db.get_table('library')
library.insert_record({'name': 'MMNN', 'books': ['kkk', 'jjj'], 'address': 'loooll'})
library.insert_record({'name': 'MMOOOOOOOOOOOOOOOOONN', 'books': ['kkk', 'jiiiil'], 'address': 'eeedcdsv'})
library.insert_record({'name': 'bboooooooooooooooob', 'books': ['kkk', 'jiiiil'], 'address': 'eeedcdsv'})
library.insert_record({'name': 'pppppppppppppppppppppppokkkkkkk', 'books': ['kkk', 'jiiiil'], 'address': 'eeedcdsv'})

book = db.get_table('book')
book.insert_record({'id': 56, 'name': 'oooo', 'writer': 'hjjjjjjjj', 'pages': 300})
book.insert_record({'id': 44, 'name': 'ttt', 'writer': 'ggg', 'pages': 300})
book.insert_record({'id': 10, 'name': 'ttt', 'writer': 'ggg', 'pages': 138})


library.delete_record('MMNN')
print(book.query_table([SelectionCriteria('pages', '=', 300)]))
book.delete_records([SelectionCriteria('pages', '=', 300)])
#print(library.query_table([SelectionCriteria('books', '=', ['kkk', 'jiiiil']), SelectionCriteria('address', '=', 'eeedcdsv')]))