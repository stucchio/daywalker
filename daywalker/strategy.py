import abc

__all__ = ['Strategy']


class Strategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def pre_open(self, dt, broker, trades, commissions):
        pass

    @abc.abstractmethod
    def pre_close(self, dt, broker, trades, commissions):
        pass
