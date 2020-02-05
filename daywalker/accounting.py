from collections import namedtuple
import pandas as pd
import numpy as np
import datetime
import pytz
if __package__ is None or __package__ == '':
    from _utils import DictableToDataframe, HasDfDict
else:
    from ._utils import DictableToDataframe, HasDfDict


__all__ = ['CostBasis', 'CapitalGainOrLoss', 'AssetAccounting', 'TradeableAsset', 'Trade']


class CostBasis(namedtuple('CostBasis', ['price', 'size', 'symbol', 'meta']), HasDfDict):
    DICT_COLUMNS = ['price', 'size', 'symbol']
    META_FIELDS = [('meta', '')]

    def cash_cost(self):
        return self.price * self.size


class CapitalGainOrLoss(namedtuple('CapitalGainOrLoss', ['open_price', 'close_price', 'size', 'symbol', 'open_meta', 'close_meta']), HasDfDict):
    DICT_COLUMNS = ['open_price', 'close_price', 'size', 'symbol']
    META_FIELDS = [('open_meta', 'open_'), ('close_meta', 'close_')]

    def cap_gain(self):
        return (self.close_price - self.open_price) * self.size


class Trade(namedtuple('Trade', ['price', 'size', 'symbol', 'date', 'meta']), HasDfDict):
    DICT_COLUMNS = ['price', 'size', 'symbol', 'date']
    META_FIELDS = [('meta', '')]

    def cash_cost(self):
        return self.price * self.size


class AssetAccounting:
    """
    This class handles the accounting for a single asset. You can buy the asset:

    >>> aa = AssetAccounting("foo")
    >>> aa.buy(price=10, size=5, meta={'foo': 'bar'})

    The parameter :param meta: is used to track assorted information, and is an arbitrary dict.
    Typical info stored here would include the purchase time, perhaps an order ID or a trade id
    (for a trade that occurs in discrete chunks).

    The quantity currently owned is available:

    >>> aa.quantity()
    5
    >>> aa.buy(11.1, 5)
    >>> aa.quantity()
    10

    Additionally, this class handles accounting for the *cost basis* (i.e. for cap gain taxes).

    >>> aa.owned()[['price', 'size', 'symbol', 'foo']]
       price  size symbol  foo
    0   10.0     5    foo  bar
    1   11.1     5    foo  NaN

    Cap gains are handled in a FIFO manner.

    Since we have no capital gains at this point, none will be returned:
    >>> aa.capital_gains()
    Empty DataFrame
    Columns: []
    Index: []

    You can also sell the asset.
    >>> aa.sell(price=12, size=3)
    >>> aa.owned()[['price', 'size', 'symbol', 'foo']]
       price  size symbol  foo
    0   10.0     2    foo  bar
    1   11.1     5    foo  NaN

    At this point there will be a capital gain.
    >>> aa.capital_gains()
       close_price open_foo  open_price  size symbol
    0         12.0      bar        10.0     3    foo

    Note how the metadata column 'foo' has been prepended with 'open_foo', since the metadata
    applied to the open leg of the trade.

    Capital gains are properly handled across tranches:
    >>> aa.sell(price=13.0, size=4)
    >>> aa.capital_gains()
       close_price open_foo  open_price  size symbol
    0         12.0      bar        10.0     3    foo
    0         13.0      bar        10.0     2    foo
    1         13.0      NaN        11.1     2    foo
    """
    def __init__(self, symbol):
        self.__owned = []
        self.__quantity = 0
        self.symbol = symbol
        self.__capital_gains_or_losses = DictableToDataframe()

    def buy(self, price, size, meta={}):
        self.__do_trade(float(price), size, meta=meta)

    def sell(self, price, size, meta={}):
        self.__do_trade(float(price), -1*size, meta=meta)

    def quantity(self):
        return self.__quantity

    def owned(self):
        result = []
        for o in self.__owned:
            result.append(o.df_dict())
        return pd.DataFrame(result)#self.__owned.copy()

    def capital_gains(self):
        return self.__capital_gains_or_losses.get()

    def __do_trade(self, price, size, meta={}):
        assert (size != 0)
        assert isinstance(meta, dict)
        self.__quantity += size
        while (np.sign(size) != 0) and (len(self.__owned) > 0):
            first = self.__owned[0]
            if np.sign(first.size) == np.sign(size):
                self.__owned.append(CostBasis(price, size, self.symbol, meta))
                size = 0
            elif abs(first.size) > abs(size):
                self.__capital_gains_or_losses.append(CapitalGainOrLoss(first.price, price, -1*size, self.symbol, first.meta, meta))
                self.__owned[0] = CostBasis(first.price, first.size + size, self.symbol, first.meta)
                size = 0
            else:
                self.__capital_gains_or_losses.append(CapitalGainOrLoss(first.price, price, first.size, self.symbol, first.meta, meta))
                self.__owned.pop(0)
                size += first.size

        if np.sign(size) != 0:
            self.__owned.append(CostBasis(price, size, self.symbol, meta))


class TradeableAsset:
    """
    This class represents a tradeable asset on an exchange with open and close auctions.

    Prices are represented via a pandas dataframe.

    >>> import pandas as pd
    >>> prices = pd.DataFrame({'date': [pd.Timestamp('2004-08-12 00:00:00-0400', tz='America/New_York'),
    ... pd.Timestamp('2004-08-13 00:00:00-0400', tz='America/New_York'),
    ... pd.Timestamp('2004-08-16 00:00:00-0400', tz='America/New_York'),
    ... pd.Timestamp('2004-08-17 00:00:00-0400', tz='America/New_York'),
    ... pd.Timestamp('2004-08-18 00:00:00-0400', tz='America/New_York')],
    ... 'open': [17.5, 17.5, 17.54, 17.35, 17.25],
    ... 'high': [17.58, 17.51, 17.54, 17.4, 17.29],
    ... 'low': [17.5, 17.5, 17.5, 17.15, 17.0],
    ... 'close': [17.5, 17.51, 17.5, 17.34, 17.11],
    ... 'volume': [2545100, 593000, 684700, 295900, 121300],
    ... 'divCash': [0.0, 0.0, 0.0, 0.0, 0.0],
    ... 'splitFactor': [1.0, 1.0, 1.0, 1.0, 1.0]})
    >>> ta = TradeableAsset('acc', prices)

    A tradeable asset will allow us to get the prices known at a given time. For example,
    before market open on 2004-08-16 (a monday), we had data from the 13'th and earlier:

    >>> price_series, open_price = ta.get_censored('2004-08-16 00:00:00-0400', after_open=False)
    >>> print(price_series.to_csv(None).replace('\\r', ''))
    date,open,high,low,close,volume,divCash,splitFactor
    2004-08-12,17.5,17.58,17.5,17.5,2545100,0.0,1.0
    2004-08-13,17.5,17.51,17.5,17.51,593000,0.0,1.0
    <BLANKLINE>

    The open price will be unavailable, since this is before the open:
    >>> open_price is None
    True

    If we do this again after the open, the open price (but not the close) is available.
    >>> price_series, open_price = ta.get_censored('2004-08-16 00:00:00-0400', after_open=True)
    >>> open_price
    17.54

    Another feature this class has is handling open/close auctions.
    >>> ta.limit_on_open('2004-08-16', price=10, size=10, is_buy=True)

    In this case, no trade is returned. That's because the open price was 17.54, so we
    did not have a match. If we submitted a price at a higher value, we'd have gotten a fill:
    >>> trade = ta.limit_on_open('2004-08-16', price=20, size=10, is_buy=True, meta={'buy_reason': '/r/wallstreetbets told me to'})
    >>> trade
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), meta={'buy_reason': '/r/wallstreetbets told me to'})

    Now lets check how much we spent:
    >>> trade.cash_cost()
    175.39999999999998

    The same works for sales.

    >>> ta.limit_on_close('2004-08-16', price=10, size=5, is_buy=False)
    Trade(price=17.5, size=-5, symbol='acc', date=Timestamp('2004-08-16 16:00:00-0400', tz='America/New_York'), meta={})

    At this point we have a capital gain.
    >>> ta.capital_gains()[['open_price', 'close_price', 'size', 'open_buy_reason', 'open_date', 'close_date', 'symbol']]  # Explicitly specifying order for unit tests
       open_price  close_price  size               open_buy_reason                 open_date                close_date symbol
    0       17.54         17.5     5  /r/wallstreetbets told me to 2004-08-16 09:30:00-04:00 2004-08-16 16:00:00-04:00    acc

    Floating point arithmetic sucks. But -0.19999999999999574 is pretty close to (17.5 - 17.54)*5=-0.20.
    """

    COLUMNS = ['open', 'high', 'low', 'close', 'volume', 'divCash', 'splitFactor']

    def __init__(self, symbol, df,
                 open_time=datetime.time(9,30,tzinfo=pytz.timezone('America/New_York')),
                 close_time=datetime.time(16,0,tzinfo=pytz.timezone('America/New_York'))):
        """
        Note that the input DF will have the timezone stripped from it's timestamps.

        However, when trades occur, they will have their time set to the open and close.
        """
        self.symbol = symbol
        self.df = df
        if 'date' in df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'].dt.date)
            self.df = df.set_index('date')[self.COLUMNS]
        self.start_date = self.df.index.min()
        self.end_date = self.df.index.max()
        self.asset_accounting = AssetAccounting(symbol)
        self.open_time = open_time
        self.close_time = close_time
        self.__trades = []

    def owned(self):
        return self.asset_accounting.owned()

    def capital_gains_or_losses(self):
        return self.asset_accounting.capital_gains_or_losses()

    def get_censored(self, dt, after_open=False):
        dt = self.df[self.df.index <= dt].index.max()

        censored = self.df[self.df.index < dt]
        if after_open:
            return (censored, self.df['open'][dt])
        else:
            return (censored, None)

    def capital_gains(self):
        return self.asset_accounting.capital_gains()

    def quantity(self):
        return self.asset_accounting.quantity()

    def trades_df(self):
        result = []
        for t in self.__trades:
            result.append(t.df_dict())
        return pd.DataFrame(result)

    def limit_on_open(self, dt, price, size, is_buy, meta={}):
        return self.__handle_auction(dt, price, size, is_buy, 'open', meta, auction_time=self.open_time)

    def limit_on_close(self, dt, price, size, is_buy, meta={}):
        return self.__handle_auction(dt, price, size, is_buy, 'close', meta, auction_time=self.close_time)

    def __copy_add_to_meta(self, meta, kv):
        meta = meta.copy()
        for (k,v) in kv:
            meta[k] = v
        return meta

    def __handle_auction(self, dt, price, size, is_buy, kind, meta={}, auction_time=None):
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
        dt_report = dt.replace(hour=auction_time.hour, minute=auction_time.minute, tzinfo=auction_time.tzinfo)
        open_price = self.df[kind][dt]
        meta = meta.copy()
        if is_buy:
            if (open_price <= price):
                t = Trade(open_price, size, self.symbol, dt_report, meta)
                self.asset_accounting.buy(open_price, size, meta=self.__copy_add_to_meta(meta, [('date', dt_report)]))
                self.__trades.append(t)
                return t
        else:
            if (open_price >= price):
                t = Trade(open_price, -1*size, self.symbol, dt_report, meta)
                self.asset_accounting.sell(open_price, size, meta=self.__copy_add_to_meta(meta, [('date', dt_report)]))
                self.__trades.append(t)
                return t


if __name__=='__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
