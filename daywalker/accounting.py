from collections import namedtuple
import pandas as pd
import numpy as np
import datetime
import numbers
import pytz
if __package__ is None or __package__ == '':
    from _utils import DictableToDataframe, HasDfDict
else:
    from ._utils import DictableToDataframe, HasDfDict


__all__ = ['CostBasis', 'CapitalGainOrLoss', 'AssetAccounting', 'TradeableAsset', 'Trade']


class CostBasis(namedtuple('CostBasis', ['price', 'size', 'symbol', 'date', 'commission_per_share', 'meta']), HasDfDict):
    DICT_COLUMNS = ['price', 'size', 'symbol', 'date', 'commission_per_share']
    META_FIELDS = [('meta', '')]

    def cost_basis_per_share(self):
        return (self.price + self.commission_per_share)

    def cash_cost(self):
        return (self.price + self.commission_per_share) * self.size


class CapitalGainOrLoss(namedtuple('CapitalGainOrLoss', ['open_price', 'close_price', 'size', 'symbol', 'open_date', 'close_date', 'open_commission_per_share', 'close_commission_per_share', 'open_meta', 'close_meta']), HasDfDict):
    DICT_COLUMNS = ['open_price', 'close_price', 'size', 'symbol', 'open_date', 'close_date', 'open_commission_per_share', 'close_commission_per_share']
    META_FIELDS = [('open_meta', 'open_'), ('close_meta', 'close_')]

    def cap_gain(self):
        return (self.close_price - self.open_price - self.open_commission - self.close_commission) * self.size


class Trade(namedtuple('Trade', ['price', 'size', 'symbol', 'date', 'commission', 'meta']), HasDfDict):
    DICT_COLUMNS = ['price', 'size', 'symbol', 'date', 'commission']
    META_FIELDS = [('meta', '')]

    def with_commission(self, commission):  # The commission gets added later, by the broker
        return Trade(self.price, self.size, self.symbol, self.date, commission, self.meta)

    def cash_cost(self):
        return (self.price * self.size) + self.commission


class AssetAccounting:
    """
    This class handles the accounting for a single asset. You can buy the asset:

    >>> aa = AssetAccounting("foo")
    >>> aa.record_trade(Trade(symbol='foo', price=10.0, size=5, commission=0, date=None, meta={'foo': 'bar'}))

    The parameter :param meta: is used to track assorted information, and is an arbitrary dict.
    Typical info stored here would include the purchase time, perhaps an order ID or a trade id
    (for a trade that occurs in discrete chunks).

    The quantity currently owned is available:

    >>> aa.quantity()
    5
    >>> aa.record_trade(Trade(symbol='foo', price=11.1, size=5, commission=1, date=None, meta={}))
    >>> aa.quantity()
    10

    Additionally, this class handles accounting for the *cost basis* (i.e. for cap gain taxes).

    >>> aa.owned()[['price', 'size', 'symbol', 'foo', 'commission_per_share']]
       price  size symbol  foo  commission_per_share
    0   10.0     5    foo  bar                   0.0
    1   11.1     5    foo  NaN                   0.2

    Cap gains are handled in a FIFO manner.

    Since we have no capital gains at this point, none will be returned:
    >>> aa.capital_gains()
    Empty DataFrame
    Columns: []
    Index: []

    You can also sell the asset.
    >>> aa.record_trade(Trade(symbol='foo', price=12.0, size=-3, commission=0.3, date=None, meta={}))
    >>> aa.owned()[['price', 'size', 'symbol', 'foo']]
       price  size symbol  foo
    0   10.0     2    foo  bar
    1   11.1     5    foo  NaN

    At this point there will be a capital gain.
    >>> aa.capital_gains()
       close_commission_per_share close_date  close_price  open_commission_per_share open_date open_foo  open_price  size symbol
    0                         0.1       None         12.0                        0.0      None      bar        10.0     3    foo

    Note how the metadata column 'foo' has been prepended with 'open_foo', since the metadata
    applied to the open leg of the trade.

    Capital gains are properly handled across tranches:
    >>> aa.record_trade(Trade(symbol='foo', price=13.0, size=-4, commission=0, date=None, meta={}))
    >>> aa.capital_gains()
       close_commission_per_share close_date  close_price  open_commission_per_share open_date open_foo  open_price  size symbol
    0                         0.1       None         12.0                        0.0      None      bar        10.0     3    foo
    0                         0.0       None         13.0                        0.1      None      bar        10.0     2    foo
    1                         0.0       None         13.0                        0.2      None      NaN        11.1     2    foo
    """
    def __init__(self, symbol):
        self.__owned = []
        self.__quantity = 0
        self.symbol = symbol
        self.__capital_gains_or_losses = DictableToDataframe()

    def __str__(self):
        return "AssetAccounting(" + self.symbol + ", quantity="+str(self.quantity()) + ")"

    def quantity(self):
        return self.__quantity

    def owned(self):
        result = []
        for o in self.__owned:
            result.append(o.df_dict())
        return pd.DataFrame(result)#self.__owned.copy()

    def capital_gains(self):
        return self.__capital_gains_or_losses.get()

    def record_trade(self, trade):
        assert (trade.symbol == self.symbol), trade.symbol + ' != ' + self.symbol
        price = float(trade.price)
        size = trade.size
        meta = trade.meta
        commission_per_share = abs(float(trade.commission / trade.size))
        assert (size != 0)
        assert isinstance(meta, dict)
        self.__quantity += size
        while (np.sign(size) != 0) and (len(self.__owned) > 0):
            first = self.__owned[0]
            if np.sign(first.size) == np.sign(size):
                self.__owned.append(CostBasis(price, size, self.symbol, commission_per_share=commission_per_share, date=trade.date, meta=meta))
                size = 0
            elif abs(first.size) > abs(size):
                self.__capital_gains_or_losses.append(CapitalGainOrLoss(first.price, price, -1*size, self.symbol,
                                                                        open_date=first.date,
                                                                        close_date=trade.date,
                                                                        open_commission_per_share=first.commission_per_share,
                                                                        close_commission_per_share=commission_per_share,
                                                                        open_meta=first.meta, close_meta=meta))
                self.__owned[0] = CostBasis(first.price, first.size + size, self.symbol, commission_per_share=commission_per_share, date=trade.date, meta=first.meta)
                size = 0
            else:
                self.__capital_gains_or_losses.append(CapitalGainOrLoss(first.price, price, first.size, self.symbol,
                                                                        open_date=first.date,
                                                                        close_date=trade.date,
                                                                        open_commission_per_share=first.commission_per_share,
                                                                        close_commission_per_share=commission_per_share,
                                                                        open_meta=first.meta, close_meta=meta))
                self.__owned.pop(0)
                size += first.size

        if np.sign(size) != 0:
            self.__owned.append(CostBasis(price, size, self.symbol, commission_per_share=commission_per_share, date=trade.date, meta=meta))


if __name__=='__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
