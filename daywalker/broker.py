import pandas as pd
from collections import namedtuple
import pytz
from functools import lru_cache
if __package__ is None or __package__ == '':
    from market_data import TradeableAsset
    from accounting import AssetAccounting
    from _utils import DictableToDataframe, DataframeBuffer
else:
    from .market_data import TradeableAsset
    from .accounting import AssetAccounting
    from ._utils import DictableToDataframe, DataframeBuffer


__all__ = ['Broker', 'BrokerInterface', 'Commission']


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
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), commission=0, meta={})

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
    def __init__(self, initial_cash, assets={}, margin=0, allow_short=False, default_timezone=pytz.timezone('America/New_York')):
        self.__cash = initial_cash
        self.__cash_vs_time = []
        self.__assets = assets
        self.__margin = margin

        self.__allow_short = allow_short
        self.__trade_callback = lambda x: None
        self.__dividends = DataframeBuffer()
        self.__trades = DictableToDataframe()

        self.__days_with_data = set()
        for k in self.__assets:
            self.__days_with_data |= self.__assets[k].trading_days()

        self.__asset_accounting = {}
        self.__capital_gains = DataframeBuffer()

        self.__default_timezone = default_timezone

        self.__asset_values = DictableToDataframe()

    def strategy_values(self):
        return self.__asset_values.get()

    @lru_cache(maxsize=1024)
    def last_price(self, symbol, dt, is_open):
        prices, open_price = self.historical_prices(symbol, dt, is_open)
        if (open_price is None):
            return prices['close'].values[-1]
        else:
            return open_price

    def positions_marked_to_market(self, dt, is_open):
        pos = self.positions().copy()
        if (len(pos) > 0):
            pos['current_value'] = pos['symbol'].apply(self.last_price, dt=dt, is_open=is_open)
            pos['market_value'] = pos['size'] * pos['current_value']
            pos['mark_to_market_time'] = pos['symbol'].apply(lambda s: self.__assets[s].date_with_time_of_day(dt, is_open))

        return pos

    def record_strategy_values(self, dt):
        result = {
            'date': dt,
            'cash': self.cash(),
            'long_equities': 0,
            'short_equities': 0,
        }

        pos = self.positions().copy()
        if len(pos) > 0:
            @lru_cache(maxsize=1024)
            def __mark_to_market(symbol):
                prices, _ = self.historical_prices(symbol, dt, False)
                return prices['close'].values[-1]

            pos['current_value'] = pos['symbol'].apply(__mark_to_market)
            pos['market_value'] = pos['size'] * pos['current_value']
            result['long_equities'] = pos[pos['size'] > 0]['market_value'].sum()
            result['short_equities'] = pos[pos['size'] < 0]['market_value'].sum()

        self.__asset_values.append(result)

    def __assets_owned(self):
        return self.__asset_accounting.keys()

    def __get_asset_accounting(self, symbol):
        symbol = symbol.lower()
        if not (symbol in self.__asset_accounting):
            self.__asset_accounting[symbol] = AssetAccounting(symbol)
        return self.__asset_accounting[symbol]

    def add_asset(self, symbol, asset):
        if isinstance(asset, TradeableAsset):
            self.__assets[symbol.lower()] = asset
        else:  # Assume asset is a dataframe of prices
            self.__assets[symbol.lower()] = TradeableAsset(symbol.lower(), asset)
        self.__days_with_data |= self.__assets[symbol.lower()].trading_days()

    def trading_day(self, dt):
        return (dt in self.__days_with_data)

    def _set_trade_callback(self, cb):
        self.__trade_callback = cb

    def allow_margin(self, final_cash):
        """Whether to allow a trade which will result in a final margin."""
        return final_cash >= (-1*self.__margin)

    def day_finished(self, dt):
        self.__cash_vs_time.append({'date': dt, 'cash': self.cash()})

    def allow_position(self, symbol, size):
        assert (symbol in self.__assets)
        return (size >= 0) or self.__allow_short

    def execute_dividends(self, dt):
        for symbol in self.__assets_owned():
            try:
                div = self.__assets[symbol].df['divCash'][dt]
            except KeyError:
                print("Skipping " + symbol + " -> " + str(dt))
                continue  # This typically happens when the symbol either wasn't live at that time, or when it wasn't a trading day
            if (div == 0):
                continue
            owned = self.__get_asset_accounting(symbol).owned()
            if len(owned) == 0:
                continue
            owned = owned.drop(columns=['price']).rename(columns={'date': 'stock_acquisition_date', 'size': 'shares'}).copy()
            if len(owned) == 0:
                continue
            owned['div_per_share'] = div
            owned['amount'] = owned['div_per_share'] * owned['shares']
            owned['ex_date'] = dt
            del owned['commission_per_share']
            self.__cash += owned['amount'].sum()
            self.__dividends.append(owned)

    def historical_prices(self, symbol, dt, after_open):
        return self.__assets[symbol].get_censored(dt, after_open)

    def dividends(self):
        return self.__dividends.get()

    def commission(self, price, size, is_buy):
        return 0

    def __update_asset_owned(self, symbol):
        symbol = symbol.lower()
        aa = self.__get_asset_accounting(symbol)
        if aa.quantity() == 0:
            self.__capital_gains.append(aa.capital_gains())
            del self.__asset_accounting[symbol]

    def positions(self):
        result = []

        for symbol in self.__assets_owned():
            result.append(self.__get_asset_accounting(symbol).owned())
        if len(result) > 0:
            return pd.concat(result)
        else:
            return pd.DataFrame()

    def limit_on_open(self, symbol, dt, price, size, is_buy, meta={}):
        t = self.__limit_on_auction(symbol, dt, price, size, is_buy, meta=meta, kind='open')
        if t:
            self.__update_asset_owned(symbol)
        return t

    def limit_on_close(self, symbol, dt, price, size, is_buy, meta={}):
        t = self.__limit_on_auction(symbol, dt, price, size, is_buy, meta=meta, kind='close')
        if t:
            self.__update_asset_owned(symbol)
        return t

    def __limit_on_auction(self, symbol, dt, price, size, is_buy, meta={}, kind=None):
        symbol = symbol.lower()
        if is_buy:
            signed_size = size
        else:
            signed_size = -1*size

        market_data = self.__assets[symbol]
        asset = self.__get_asset_accounting(symbol)
        final_position = asset.quantity() + signed_size
        if not self.allow_position(symbol, final_position):
            return None
        if is_buy:
            final_cash = self.cash() - price*size
        else:
            final_cash = self.cash() + price*size
        if not self.allow_margin(final_cash):
            return None

        if (kind == 'open'):
            trade = market_data.limit_on_open(dt, price, size, is_buy, meta=meta)
        elif (kind == 'close'):
            trade = market_data.limit_on_close(dt, price, size, is_buy, meta=meta)
        else:
            trade = None
        if trade:
            commission = self.commission(trade.price, trade.size, trade.size > 0)
            trade = trade.with_commission(commission)
            self.__cash -= trade.cash_cost()
            self.__get_asset_accounting(symbol.lower()).record_trade(trade)
            self.__append_trade(trade)
        return trade

    def __append_trade(self, trade):
        self.__trade_callback(trade)
        self.__trades.append(trade)

    def cash(self):
        return self.__cash

    def cash_vs_time(self):
        return pd.DataFrame(self.__cash_vs_time).set_index('date')

    def capital_gains(self):
        result = []
        for a in self.__asset_accounting.values():
            result.append(a.capital_gains())
        result.append(self.__capital_gains.get())
        result = pd.concat(result)
        result['gain'] = ((result['close_price'] - result['open_price'] - result['close_commission_per_share'] - result['open_commission_per_share'])*result['size'])
        return result

    def trades(self):
        return self.__trades.get()

    def trades_df(self):
        print("Deprecated")
        return self.trades()


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
    Trade(price=17.54, size=10, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), commission=1.0, meta={'trade_id': 'bar'})

    >>> b.cash()
    999823.6

    >>> b.limit_on_open('acc', '2004-08-16', price=50, size=350, is_buy=True, meta={'trade_id': 'foo'})
    Trade(price=17.54, size=350, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), commission=1.75, meta={'trade_id': 'foo'})
    >>> b.limit_on_open('acc', '2004-08-16', price=50, size=1, is_buy=True, meta={'trade_id': 'foo'})
    Trade(price=17.54, size=1, symbol='acc', date=Timestamp('2004-08-16 09:30:00-0400', tz='America/New_York'), commission=0.1754, meta={'trade_id': 'foo'})


    Simple arithmetic suggests that cash should be 10000 - 17.54*10 = 9824.6, but the commission is also
    taken into account.
    >>> b.trades()[['price', 'size', 'symbol', 'date', 'trade_id', 'commission']]
       price  size symbol                      date trade_id  commission
    0  17.54    10    acc 2004-08-16 09:30:00-04:00      bar      1.0000
    1  17.54   350    acc 2004-08-16 09:30:00-04:00      foo      1.7500
    2  17.54     1    acc 2004-08-16 09:30:00-04:00      foo      0.1754
    """

    def commission(self, price, size, is_buy):
        return min(max(1.0, 0.005*abs(size)), 0.01*abs(size)*price)


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
       commission                      date  price  size symbol trade_id
    0         1.0 2004-08-17 09:30:00-04:00  17.35    10    acc      bar
    """

    def __init__(self, broker, dt, after_open=False):
        self.__broker = broker
        self.__dt = dt
        self.__after_open = after_open
        self.__trades_to_report = []
        self.__broker._set_trade_callback(lambda t: self.__trades_to_report.append(t))

    def cash(self):
        return self.__broker.cash()

    def set_date(self, dt, after_open):
        self.__dt = dt
        self.__after_open = after_open
        self.__positions = self.__broker.positions()
        self.__positions_marked_to_market = self.__broker.positions_marked_to_market(self.__dt, self.__after_open)
        if (after_open == False):  # Dividends take effect on the ex-dividend date
            self.__broker.execute_dividends(self.__dt)

    def positions(self):
        return self.__positions

    def get_unreported_items(self):
        trades = self.__trades_to_report
        trades = pd.DataFrame([t.df_dict() for t in trades])
        self.__trades_to_report = []
        return trades

    def historical_prices(self, symbol):
        return self.__broker.historical_prices(symbol, self.__dt, self.__after_open)

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

    def last_price(self, symbol):
        return self.__broker.last_price(symbol, self.__dt, self.__after_open)

    def positions_marked_to_market(self):
        return self.__positions_marked_to_market



if __name__=='__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
