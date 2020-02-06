import abc

__all__ = ['Strategy']


class Strategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def pre_open(self, dt, broker, trades, commissions, other_data):
        pass

    @abc.abstractmethod
    def pre_close(self, dt, broker, trades, commissions, other_data):
        pass
