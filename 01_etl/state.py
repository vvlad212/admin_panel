import abc
from typing import Any, Optional
import json


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = '/Users/vladislavzujkov/YandexPracticum/new_admin_panel_sprint_3/01_etl/state.json'#file_path

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""
        with open(self.file_path, 'r') as file:
            state_file = file.read()
            if state_file != '':
                return json.loads(state_file)
            else:
                return {}

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(state))


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с
    БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state_dict = self.storage.retrieve_state()
        if state_dict.keys().__contains__(key):
            return state_dict[key]
        else:
            return None


if __name__ == '__main__':
    # storage = JsonFileStorage()
    # storage.file_path = '/Users/vladislavzujkov/PycharmProjects/Yandex_learn/test.json'
    # state = State(storage).get_state('modifie')
    # State(storage).set_state('1', 'sstate')
    pass
