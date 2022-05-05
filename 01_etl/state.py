import abc
import json
import os
from abc import ABC
from typing import Any, Optional


class BaseStorage(ABC):

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище

        Args:
            state:
        """

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""


class JsonFileStorage(BaseStorage):

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""
        result = {}
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                state_file = file.read()
                if state_file != '':
                    result = json.loads(state_file)
        return result

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(state))


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.178
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state_dict = self.storage.retrieve_state()
        return state_dict.get(key)
