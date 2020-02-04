# Daywalker backtesting framework

Daywalker is a stock market backtesting framework. I wrote it mainly for my own personal use because I was unsatisfied with existing frameworks.

There's a lot of these, so let me outline my design goals/methodology explicitly.

- I want to do as much of my analysis as possible in Python/Pandas/Numpy.
- I only trade daily (*no intraday*), almost 100% limit-on-open and limit-on-close. Many backtesting frameworks don't support these kinds of orders.
- My strategies run on a wide range of stocks and use alternative data sources. Providing a censored view of many symbols is important to me.
- Transaction costs, dividends, and taxes need to be tracked properly.
- I want to easily add tracking information and useful statistics to my trades, and get this out at the end.

## Cost basis, sources of funds and taxes

Taxes matter a lot. There are a number of trading strategies that sacrifice a little alpha in order to minimize taxes, and justifiably so. Tax loss harvesting, as practiced by assorted robo-advisors (e.g. Wealthfront) is a good example.

In order to measure taxes, the source of income must be tracked. Dividends are treated differently from long and short term capital gains, for example.

For this reason, Daywalker tracks the cost-basis of every purchase you make.

For example, suppose you buy 100@25 of X at 100 on Jan 1, 1990 and another 100@30 shares on June 1, 1990. On Jan 10, 1991, you sell 150@35. The result, in terms of capital gains, is:
- A long term capital gain of `100 * (35 - 25) = 1000`.
- A short term capital gain of `50 x (35 - 30) = 250`.

The $1000 cap gain is taxed at a low rate (in the US), while the $250 cap gain is taxed at a much higher rate. It might be a worthwhile improvement to the strategy to sell only the 100@35 to minimize capital gains.

## Tracking information/trade metadata

The tracking information is an important piece here that I want to explain further. Suppose I am running a long/short strategy of the following nature - when an event occurs, I will open both a long and a short.

The way this works in daywalker is as follows. Inside my `Strategy` object, I will enter trades in the following manner:

    def pre_open(self, dt, broker, trades, commissions):
        story = get_wallstreetbets_recommendation(dt)
        story_score = get_story_score(story)
        if story_score > self.threshold:
            long_symbol, short_symbol = get_symbols_from_story(story)

            broker.limit_on_open(long_symbol, price=..., size=..., is_buy=True, meta={ 'trade_story_id': story.id })
            broker.limit_on_open(short_symbol, price=..., size=..., is_buy=False, meta={ 'trade_story_id': story.id })

The key observation here is that what's important to me is the P&L of the combined long/short position. One metric I will want to track is the Sharpe ratio of my strategy, computed as the average *over individual long/short trades* (rather than over time).

After we run the simulation we can use the metadata to track our trades.

    >>> cap_gains = m.broker.capital_gains()
    >>> cap_gains
       open_price  close_price  size open_trade_story_id                  open_date close_trade_id                close_date    symbol
    0       17.50        17.51     1             1        2004-08-12 09:30:00-04:00              0 2004-08-13 16:00:00-04:00    "long_symbol"
    1       17.50        17.50     2             1        2004-08-13 09:30:00-04:00              1 2004-08-16 16:00:00-04:00    "short_symbol"
    2       17.54        17.34     3             2        2004-08-16 09:30:00-04:00              0 2004-08-17 16:00:00-04:00    "another_long_symbol_from_a_diff_event"
    ...

We can now take our `cap_gains` dataframe and do `cap_gains.groupby('open_trade_story_id')` to do an analysis on the level of an individual story (where each story triggered multiple trades). This makes it easy to analyze our capital gains from the *combined long/short* position.

Without the ability to assign the same tag to both trades, we'd need to write additional code
