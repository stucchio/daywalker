import unittest
import pandas as pd
from daywalker import TradeableAsset, Market, Strategy
from daywalker.broker import InteractiveBrokers
import sys

class TestStrategy(Strategy):
    """This is used just for the doctests."""
    def __init__(self):
        self.bought = False

    def pre_open(self, dt, broker, trades, other_data):
        self.pre_opened = True
        if not self.bought:
            broker.limit_on_open('acc', price=10, size=10, is_buy=True, meta={})
            self.bought = True

    def pre_close(self, dt, broker, trades, other_data):
        return None



class TestMarket(unittest.TestCase):
    def test_split1(self):
        prices = pd.DataFrame({'date': [pd.Timestamp('2004-08-12 00:00:00-0400', tz='America/New_York'), pd.Timestamp('2004-08-13 00:00:00-0400', tz='America/New_York'),
                                        pd.Timestamp('2004-08-16 00:00:00-0400', tz='America/New_York'), pd.Timestamp('2004-08-17 00:00:00-0400', tz='America/New_York'),
                                        pd.Timestamp('2004-08-18 00:00:00-0400', tz='America/New_York')],
                                'open': [10, 10, 10, 5, 5],
                                'high': [10, 10, 10, 5, 5],
                                'low': [10, 10, 10, 5, 5],
                                'close': [10, 10, 10, 5, 5],
                                'volume': [2545100, 593000, 684700, 295900, 121300],
                                'divCash': [0.0, 0.0, 0.0, 0.00, 0.0],
                                'splitFactor': [1.0, 1.0, 1.0, 2.0, 1.0]})
        ta = TradeableAsset('acc', prices)
        b = InteractiveBrokers(10*1000, {'acc': ta})
        strat = TestStrategy()
        m = Market(prices['date'].min(), prices['date'].max(), strat, b)

        m.run()
        positions = m.broker.positions()
        self.assertEqual(len(positions), 1)
        # Due to the split, the position should now be 20
        self.assertEqual(positions['size'][0], 20)
        # Value of equities should remain a flat 100 - the split changes nothing
        values = m.broker.strategy_values()
        self.assertTrue((values['cash'] == 9899).all())  # Cash should be 100 to purchase securities - commission of $1
        self.assertTrue((values['long_equities'] == 100).all())
