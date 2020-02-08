from collections import namedtuple
import pandas as pd
import numpy as np
import datetime
import numbers
import pytz
if __package__ is None or __package__ == '':
    from _utils import DictableToDataframe, HasDfDict
    from accounting import Trade
else:
    from ._utils import DictableToDataframe, HasDfDict
    from .accounting import Trade


class TradeableAsset:
    """
    This class represents market data for a tradeable asset on an exchange with open and close auctions.

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
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), commission=0, meta={'buy_reason': '/r/wallstreetbets told me to'})

    Now lets check how much we spent:
    >>> trade.cash_cost()
    175.39999999999998

    The same works for sales.

    >>> ta.limit_on_close('2004-08-16', price=10, size=5, is_buy=False)
    Trade(price=17.5, size=-5, symbol='acc', date=Timestamp('2004-08-16 16:00:00-0400', tz='America/New_York'), commission=0, meta={})

    Floating point arithmetic sucks. But -0.19999999999999574 is pretty close to (17.5 - 17.54)*5=-0.20.

    Note that when a trade is computed via this class, **it is not recorded anywhere.** This class
    just handles market data and the possibility of a trade.
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
        self.open_time = open_time
        self.close_time = close_time

    def get_censored(self, dt, after_open=False):
        dt = self.df[self.df.index <= dt].index.max()

        censored = self.df[self.df.index < dt]
        if after_open:
            return (censored, self.df['open'][dt])
        else:
            return (censored, None)

    def trading_days(self):
        return set(self.df.index)

    def limit_on_open(self, dt, price, size, is_buy, *, meta={}):
        return self.__handle_auction(dt, price, size, is_buy, 'open', meta, auction_time=self.open_time)

    def limit_on_close(self, dt, price, size, is_buy, *, meta={}):
        return self.__handle_auction(dt, price, size, is_buy, 'close', meta, auction_time=self.close_time)

    def __copy_add_to_meta(self, meta, kv):
        meta = meta.copy()
        for (k,v) in kv:
            meta[k] = v
        return meta

    def date_with_time_of_day(self, dt, after_open):
        if after_open:
            return dt.replace(hour=self.close_time.hour, minute=self.close_time.minute, tzinfo=self.close_time.tzinfo)
        else:
            return dt.replace(hour=self.open_time.hour, minute=self.open_time.minute, tzinfo=self.open_time.tzinfo)

    def __handle_auction(self, dt, price, size, is_buy, kind, meta={}, auction_time=None):
        """
        This will return a trade that the market would execute.

        This class does NOT actually handle the trade itself.
        """
        assert isinstance(price, numbers.Number)
        assert isinstance(size, numbers.Number)
        assert (kind == 'close') or (kind == 'open')
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
        dt_report = dt.replace(hour=auction_time.hour, minute=auction_time.minute, tzinfo=auction_time.tzinfo)
        open_price = self.df[kind][dt]
        meta = meta.copy()
        if is_buy:
            if (open_price <= price):
                t = Trade(open_price, size, self.symbol, dt_report, commission=0, meta=meta)
                return t
        else:
            if (open_price >= price):
                t = Trade(open_price, -1*size, self.symbol, dt_report, commission=0, meta=meta)
                return t


if __name__=='__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
