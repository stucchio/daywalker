if __package__ is None or __package__ == '':
    from accounting import *
    from _utils import DictableToDataframe, DataframeBuffer
else:
    from .accounting import *
    from ._utils import DictableToDataframe, DataframeBuffer
import pandas as pd
from collections import namedtuple


__all__ = ['Broker', 'BrokerInterface', 'Commission']

class Commission(namedtuple('Commission', ['trade', 'amount'])):
    def df_dict(self):
        d = self.trade.df_dict()
        d['commission'] = self.amount
        return d

class Broker:
    """
    This class handles broker requirements.

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
    ... 'divCash': [0.0, 0.0, 0.0, 0.25, 0.0],
    ... 'splitFactor': [1.0, 1.0, 1.0, 1.0, 1.0]})
    >>> ta = TradeableAsset('acc', prices)

    >>> b = Broker(10000, {'acc': TradeableAsset('acc', prices)})
    >>> b.allow_margin(-5)
    False
    >>> b2 = Broker(10000, {'acc': TradeableAsset('acc', prices)}, margin=10000)
    >>> b2.allow_margin(-5000)
    True
    >>> b2.allow_margin(-15000)
    False

    A broker allows one to purchase an asset.
    >>> b.cash()
    10000
    >>> b.limit_on_open('acc', '2004-08-16', price=10, size=10, is_buy=True)

    No order went through. Our cash remains the same.
    >>> b.cash()
    10000

    >>> b.limit_on_open('acc', '2004-08-16', price=50, size=10, is_buy=True)
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), meta={})

    This time there was a trade.
    >>> b.cash() == (10000 - 17.54*10)
    True

    We also handle dividends.

    >>> old_cash = b.cash()
    >>> b.execute_dividends(pd.Timestamp('2004-08-17'))
    >>> div = b.dividends()
    >>> div[['amount', 'div_per_share', 'ex_date', 'shares', 'symbol']]
       amount  div_per_share    ex_date  shares symbol
    0     2.5           0.25 2004-08-17      10    acc

    After issuing dividends, the amount of cash should increase by the appropriate amount.

    >>> b.cash() == (old_cash + div['amount'][0])
    True
    """
    def __init__(self, initial_cash, assets, margin=0, allow_short=False):
        self.__cash = initial_cash
        self.__cash_vs_time = []
        self.__assets = assets
        self.__margin = margin

        self.__allow_short = allow_short
        self.__trade_callback = lambda x: None
        self.__commission_callback = lambda x: None
        self.__assets_owned = set()
        self.__commissions = DictableToDataframe()
        self.__dividends = DataframeBuffer()

    def _set_trade_callback(self, cb):
        self.__trade_callback = cb

    def _set_commission_callback(self, cb):
        self.__commission_callback = cb

    def allow_margin(self, final_cash):
        """Whether to allow a trade which will result in a final margin."""
        return final_cash >= (-1*self.__margin)

    def day_finished(self, dt):
        self.__cash_vs_time.append({'date': dt, 'cash': self.cash()})

    def allow_position(self, symbol, size):
        assert (symbol in self.__assets)
        return (size >= 0) or self.__allow_short

    def execute_dividends(self, dt):
        for symbol in self.__assets_owned:
            div = self.__assets[symbol].df['divCash'][dt]
            if (div == 0):
                continue
            owned = self.__assets[symbol].owned().drop(columns=['price'])
            owned = owned.rename(columns={'date': 'stock_acquisition_date', 'size': 'shares'}).copy()
            if len(owned) == 0:
                continue
            owned['div_per_share'] = div
            owned['amount'] = owned['div_per_share'] * owned['shares']
            owned['ex_date'] = dt
            self.__cash += owned['amount'].sum()
            self.__dividends.append(owned)

    def dividends(self):
        return self.__dividends.get()

    def commission(self, trade):
        return None

    def commissions(self):
        return self.__commissions.get()

    def __update_asset_owned(self, symbol):
        if self.__assets[symbol].quantity() != 0:
            self.__assets_owned.add(symbol)
        else:
            self.__assets_owned.discard(symbol)

    def positions(self):
        result = []
        for symbol in self.__assets_owned:
            result.append(self.__assets[symbol].owned())
        if len(result) > 0:
            return pd.concat(result)
        else:
            return pd.DataFrame()

    def limit_on_open(self, symbol, dt, price, size, is_buy, meta={}):
        t = self.__limit_on_auction(symbol, dt, price, size, is_buy, meta, 'open')
        if t:
            self.__update_asset_owned(symbol)
        return t

    def limit_on_close(self, symbol, dt, price, size, is_buy, meta={}):
        t = self.__limit_on_auction(symbol, dt, price, size, is_buy, meta, 'close')
        if t:
            self.__update_asset_owned(symbol)
        return t

    def historical_prices(self, symbol, dt, after_open):
        return self.__assets[symbol].get_censored(dt, after_open)

    def __limit_on_auction(self, symbol, dt, price, size, is_buy, meta={}, kind=None):
        if is_buy:
            signed_size = size
        else:
            signed_size = -1*size

        asset = self.__assets[symbol]
        final_position = asset.quantity() + signed_size
        if not self.allow_position(symbol, final_position):
            return None
        final_cash = self.cash() - price*size
        if not self.allow_margin(final_cash):
            return None

        if (kind == 'open'):
            trade = asset.limit_on_open(dt, price, size, is_buy, meta)
        elif (kind == 'close'):
            trade = asset.limit_on_close(dt, price, size, is_buy, meta)
        else:
            trade = None
        if trade:
            self.__cash -= trade.cash_cost()
            comm = self.commission(trade)
            if comm:
                self.__cash -= comm.amount
                self.__append_commission(comm)

        if trade:
            self.__append_trade(trade)
        return trade

    def __append_commission(self, commission):
        self.__commissions.append(commission)
        self.__commission_callback(commission)

    def __append_trade(self, trade):
        self.__trade_callback(trade)

    def cash(self):
        return self.__cash

    def cash_vs_time(self):
        return pd.DataFrame(self.__cash_vs_time).set_index('date')

    def capital_gains_or_losses(self):
        result = []
        for a in self.__assets.values():
            result += a.capital_gains_or_losses()
        return result

    def capital_gains(self):
        result = []
        for a in self.__assets.values():
            result.append(a.capital_gains())
        return pd.concat(result)

    def trades_df(self):
        result = []
        for a in self.__assets.values():
            result.append(a.trades_df())
        return pd.concat(result)


class InteractiveBrokers(Broker):
    """
    This class includes the Interactive Brokers commission on the Pro plan.

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

    >>> b = InteractiveBrokers(1000000, {'acc': TradeableAsset('acc', prices)})
    >>> b.limit_on_open('acc', '2004-08-16', price=50, size=10, is_buy=True, meta={'trade_id': 'bar'})
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), meta={'trade_id': 'bar'})

    >>> b.cash()
    999823.6

    >>> b.limit_on_open('acc', '2004-08-16', price=50, size=350, is_buy=True, meta={'trade_id': 'foo'})
    Trade(price=17.54, size=350, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), meta={'trade_id': 'foo'})

    Simple arithmetic suggests that cash should be 10000 - 17.54*10 = 9824.6, but the commission is also
    taken into account.
    >>> b.commissions()[['price', 'size', 'symbol', 'date', 'trade_id', 'commission']]
       price  size symbol                      date trade_id  commission
    0  17.54    10    acc 2004-08-16 09:30:00-04:00      bar        1.00
    1  17.54   350    acc 2004-08-16 09:30:00-04:00      foo        1.75
    """

    def commission(self, trade):
        amt = min(max(1.0, 0.005*abs(trade.size)), 0.01*abs(trade.size)*trade.price)
        return Commission(trade, amt)

class BrokerException(Exception):
    pass

class InvalidOrderException(BrokerException):
    pass

class BrokerInterface:
    """
    This class is a wrapper around your broker, which is exposed to the strategy.

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

    >>> b = InteractiveBrokers(1000000, {'acc': TradeableAsset('acc', prices)})
    >>> b = BrokerInterface(b, pd.to_datetime('2004-08-17'))
    >>> b.limit_on_open('acc', price=50, size=10, is_buy=True, meta={'trade_id': 'bar'})

    Unlike the broker, the BrokerInterface does not actually return the trade. This is because
    the BrokerInterface *accepts* the order. However, the order is not executed until *after*
    market open/close.

    After market open, these will be available from the get_unreported_items() method.

    >>> b.get_unreported_items()
    ([Trade(price=17.35, size=10, symbol='acc', date=Timestamp('2004-08-17 09:30:00-0400', tz='America/New_York'), meta={'trade_id': 'bar'})], [Commission(trade=Trade(price=17.35, size=10, symbol='acc', date=Timestamp('2004-08-17 09:30:00-0400', tz='America/New_York'), meta={'trade_id': 'bar'}), amount=1.0)])
    """
    def __init__(self, broker, dt, after_open=False):
        self.__broker = broker
        self.__dt = dt
        self.__after_open = after_open
        self.__trades_to_report = []
        self.__commissions_to_report = []
        self.__broker._set_trade_callback(lambda t: self.__trades_to_report.append(t))
        self.__broker._set_commission_callback(lambda c: self.__commissions_to_report.append(c))

    def set_date(self, dt, after_open):
        self.__dt = dt
        self.__after_open = after_open
        self.__positions = self.__broker.positions()
        if (after_open == False):  # Dividends take effect on the ex-dividend date
            self.__broker.execute_dividends(self.__dt)

    def positions(self):
        return self.__positions

    def get_unreported_items(self):
        trades = self.__trades_to_report
        self.__trades_to_report = []
        commissions = self.__commissions_to_report
        self.__commissions_to_report = []
        return (trades, commissions)

    def commission(self, trade):
        return self.broker.commission(trade)

    def commissions(self):
        return self.broker.commissions()

    def limit_on_open(self, symbol, price, size, is_buy, meta={}):
        if (not self.__after_open):
            self.__broker.limit_on_open(symbol, self.__dt, price, size, is_buy, meta)
        else:
            raise InvalidOrderException("The open has already passed. You must submit a limit_on_close order.")

    def limit_on_close(self, symbol, price, size, is_buy, meta={}):
        if (self.__after_open):
            self.__broker.limit_on_close(symbol, self.__dt, price, size, is_buy, meta)
        else:
            raise InvalidOrderException("You can't submit a limit_on_close order until after the open.")

    def historical_prices(self, symbol):
        return self.__assets[symbol].get_censored(self.__dt, self.__after_open)


if __name__=='__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
