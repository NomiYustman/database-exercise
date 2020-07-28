import json
from pathlib import Path
from typing import Any


DB_ROOT = Path('db_files')


def create_json_file(file_name: str):
    return open(DB_ROOT / file_name, 'w')


def insert_dict_to_json_file(file, dictionary: dict) -> None:
    json.dump(dictionary, file)


def read_json_file(file_name: str) -> dict:
    with open(DB_ROOT / file_name, 'r') as file:
        dict_ = json.load(file)
    return dict_


def write_to_json_file(file_name: str, key: Any, value: Any) -> None:
    dictionary = read_json_file(file_name)
    dictionary[key] = value
    with open(DB_ROOT / file_name, 'w') as file:
        json.dump(dictionary, file)


def delete_from_json_file(file_name: str, keys: list) -> None:
    dictionary = read_json_file(file_name)
    for key in keys:
        dictionary.pop(key)
    with open(DB_ROOT / file_name, 'w+') as file:
        json.dump(dictionary, file)


