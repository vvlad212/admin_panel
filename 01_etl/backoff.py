import logging
import time
from functools import wraps


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            n = factor
            while True:
                try:
                    return func(*args, *kwargs)
                except Exception as ex:
                    logging.exception(ex)
                    logging.info(f"Attempt {n}")
                    if t < border_sleep_time:
                        t = start_sleep_time * (2 ** n)
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    n += 1
                    time.sleep(t)

        return inner

    return func_wrapper
