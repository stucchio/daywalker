import abc
from collections import defaultdict
from ._utils import DictableToDataframe

__all__ = ['Strategy']


class Strategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def pre_open(self, dt, broker, trades, other_data):
        pass

    @abc.abstractmethod
    def pre_close(self, dt, broker, trades, other_data):
        pass

    def __logs(self):
        if not hasattr(self, '_logs'):
            self._logs = defaultdict(DictableToDataframe)
        return self._logs

    def log(self, name, data, dt):
        data['date'] = dt
        self.__logs()[name].append(data)

    def get_log(self, name):
        return self.__logs()[name].get()
