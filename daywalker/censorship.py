import pandas as pd
import pytz
import datetime


__all__ = ['CensoredView']


class CensoredView:
    def __init__(self, df, censor_on_index=True, censor_column=None, default_timezone=pytz.timezone('America/New_York')):
        assert (censor_on_index or (censor_column is not None)), "A column controlling the censorship time must be specified."
        self.df = df
        self.censor_on_index = censor_on_index
        self.censor_column = censor_column
        self.default_timezone = default_timezone

    def get_censored(self, dt):
        dt = pd.to_datetime(dt)
        if (dt.tz is None):
            dt = dt.replace(tzinfo=self.default_timezone)
        if self.censor_on_index:
            dt = self.df[self.df.index <= dt].index.max()
            return self.df[self.df.index < dt]
        else:
            return self.df[self.df[self.censor_column] <= dt]


class CensoredData:
    def __init__(self):
        self.__data = {}
        self.__dt = None

    def add_data(self, name, data, censor_on_index=True, censor_column=None):
        if isinstance(data, CensoredView):
            self.__data[name] = data
        else:
            self.__data[name] = CensoredView(data, censor_on_index=censor_on_index, censor_column=censor_column)

    def set_date(self, dt):
        self.__dt = dt

    def get_data(self, name):
        return self.__data[name].get_censored(self.__dt)
