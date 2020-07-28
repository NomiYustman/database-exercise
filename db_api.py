import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type

from dataclasses_json import dataclass_json

from files_functions import read_json_file, write_to_json_file, delete_from_json_file

DB_ROOT = Path('db_files')


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str
    counter: int = 0

    def count(self) -> int:
        return self.counter

    def insert_record(self, values: Dict[str, Any]) -> None:
        self.counter += 1
        write_to_json_file(self.name + '.json', values[self.key_field_name], values)

    def delete_record(self, key: Any) -> None:
        self.counter -= 1
        delete_from_json_file(self.name + '.json', [key])

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        keys = [f'{row[self.key_field_name]}' for row in self.query_table(criteria)]
        self.counter -= len(keys)
        delete_from_json_file(self.name + '.json', keys)

    def get_record(self, key: Any) -> Dict[str, Any]:
        table = read_json_file(self.name + '.json')
        return table[key]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        write_to_json_file(self.name + '.json', key, values)

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        ret_list = []
        table = read_json_file(self.name + '.json')
        for row_key in table:
            for criterion in criteria:
                if not is_expression_true(table[row_key][criterion.field_name], criterion.operator, criterion.value):
                    break
            else:
                ret_list += [table[row_key]]

        return ret_list

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


def is_expression_true(lhs: Any, operator: str, rhs: Any) -> bool:
    if operator == '=':
        operator = '=='
    return eval(str(lhs) + operator + str(rhs))
