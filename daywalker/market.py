if __package__ is None or __package__ == '':
    from accounting import *
    from broker import Broker, BrokerInterface, InteractiveBrokers
    from strategy import Strategy
    from censorship import CensoredData
else:
    from .accounting import *
    from .broker import Broker, BrokerInterface, InteractiveBrokers
    from .strategy import Strategy
    from .censorship import CensoredData
import pandas as pd
import abc

__all__ = ['Market']


class _TestStrategy(Strategy):
    """This is used just for the doctests."""
    def __init__(self, symbol):
        self.symbol = symbol
        self.size = 1

    def pre_open(self, dt, broker, trades, commissions, other_data):
        broker.limit_on_open('acc', price=100, size=self.size, is_buy=True, meta={'trade_id': str(self.size % 2)})

    def pre_close(self, dt, broker, trades, commissions, other_data):
        if (self.size <= 4):
            if (self.size > 1):
                broker.limit_on_close('acc', price=10, size=self.size-1, is_buy=False, meta={'trade_id': str(self.size % 2)})
        self.size += 1


class Market:
    """
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
    ... 'divCash': [0.0, 0.0, 0.0, 0.10, 0.0],
    ... 'splitFactor': [1.0, 1.0, 1.0, 1.0, 1.0]})
    >>> ta = TradeableAsset('acc', prices)
    >>> b = InteractiveBrokers(10000, {})
    >>> m = Market(prices['date'].min(), prices['date'].max(), _TestStrategy('acc'), b)

    Assets can also be added to the broker/market after creation if this is easier:
    >>> m.add_asset('acc', prices)

    Finally a backtest can be run:
    >>> m.run()

    After running the simulation, we hold the following positions:
    >>> m.broker.positions()[['price', 'size', 'symbol', 'trade_id', 'date']]
       price  size symbol trade_id                      date
    0  17.35     4    acc        0 2004-08-17 09:30:00-04:00
    1  17.25     5    acc        1 2004-08-18 09:30:00-04:00

    Note that what is reported includes all the different potential cost bases.

    Capital gains can similarly be found:
    >>> cg = m.broker.capital_gains()
    >>> cg[['open_price', 'close_price', 'size', 'open_trade_id', 'open_date', 'close_trade_id', 'close_date', 'symbol']]
       open_price  close_price  size open_trade_id                 open_date close_trade_id                close_date symbol
    0       17.50        17.51     1             1 2004-08-12 09:30:00-04:00              0 2004-08-13 16:00:00-04:00    acc
    1       17.50        17.50     2             0 2004-08-13 09:30:00-04:00              1 2004-08-16 16:00:00-04:00    acc
    2       17.54        17.34     3             1 2004-08-16 09:30:00-04:00              0 2004-08-17 16:00:00-04:00    acc

    As well as commissions that were paid.
    >>> commissions = m.broker.commissions()
    >>> commissions[['price', 'size', 'symbol', 'date', 'trade_id', 'commission']]
       price  size symbol                      date trade_id  commission
    0  17.50     1    acc 2004-08-12 09:30:00-04:00        1      0.1750
    1  17.50     2    acc 2004-08-13 09:30:00-04:00        0      0.3500
    2  17.51    -1    acc 2004-08-13 16:00:00-04:00        0      0.1751
    3  17.54     3    acc 2004-08-16 09:30:00-04:00        1      0.5262
    4  17.50    -2    acc 2004-08-16 16:00:00-04:00        1      0.3500
    5  17.35     4    acc 2004-08-17 09:30:00-04:00        0      0.6940
    6  17.34    -3    acc 2004-08-17 16:00:00-04:00        0      0.5202
    7  17.25     5    acc 2004-08-18 09:30:00-04:00        1      0.8625


    Accounting identities that should remain true:

    >>> div = m.broker.dividends()

    >>> m.broker.cash()
    9840.407
    >>> cap_gain = ((cg['close_price'] - cg['open_price'])*cg['size']).sum()
    >>> cap_gain
    -0.5899999999999963

    >>> trades = m.broker.trades_df()
    >>> trades[['price', 'size', 'symbol', 'date', 'trade_id']]
       price  size symbol                      date trade_id
    0  17.50     1    acc 2004-08-12 09:30:00-04:00        1
    1  17.50     2    acc 2004-08-13 09:30:00-04:00        0
    2  17.51    -1    acc 2004-08-13 16:00:00-04:00        0
    3  17.54     3    acc 2004-08-16 09:30:00-04:00        1
    4  17.50    -2    acc 2004-08-16 16:00:00-04:00        1
    5  17.35     4    acc 2004-08-17 09:30:00-04:00        0
    6  17.34    -3    acc 2004-08-17 16:00:00-04:00        0
    7  17.25     5    acc 2004-08-18 09:30:00-04:00        1

    >>> div = m.broker.dividends()
    >>> div
         stock_acquisition_date  shares symbol trade_id  div_per_share  amount    ex_date
    0 2004-08-16 09:30:00-04:00       3    acc        1            0.1     0.3 2004-08-17

    Accounting identity: initial_cash = cash + gain/loss from trades + commissions - dividends
    >>> m.broker.cash() + (trades['price']*trades['size']).sum() + commissions['commission'].sum() - div['amount'].sum()
    10000.0
    """
    def __init__(self, start_date, end_date, strategy, broker, other_data=None):
        self.start_date = start_date
        self.end_date = end_date
        self.strategy = strategy
        self.broker = broker
        if other_data is None:
            self.other_data = CensoredData()
        elif isinstance(other_data, CensoredData):
            self.other_data = other_data
        else:
            raise ValueError("other_data argument must be an instance of CensoredData. You passed in a " + str(type(other_data)))

    def add_asset(self, symbol, asset):
        self.broker.add_asset(symbol, asset)

    def run(self):
        dt = self.start_date
        bi = BrokerInterface(self.broker, dt, after_open=False)
        while (dt <= self.end_date):
            bi.set_date(dt, False)
            trades, commissions = bi.get_unreported_items()
            self.strategy.pre_open(dt, bi, trades, commissions, self.other_data)
            bi.set_date(dt, True)
            self.other_data.set_date(dt)
            trades, commissions = bi.get_unreported_items()
            self.strategy.pre_close(dt, bi, trades, commissions, self.other_data)
            dt = dt + pd.offsets.BDay()


if __name__ == '__main__':
    import sys
    sys.path.append('.')
    import doctest
    doctest.testmod()
