import pandas as pd


__all__ = ['CensoredView']


class CensoredView:
    def __init__(self, df, censor_on_index=True, censor_column=None):
        assert (censor_on_index or (censor_column is not None)), "A column controlling the censorship time must be specified."
        self.df = df
        self.censor_on_index = censor_on_index
        self.censor_column = censor_column

    def get_censored(self, dt):
        if self.censor_on_index:
            dt = self.df[self.df.index <= dt].index.max()
        else:
            dt = self.df[self.df[self.censor_column] <= dt][self.censor_column].max()

        return self.df[self.df.index < dt]


class CensoredData:
    def __init__(self):
        self.__data = {}
        self.__dt = None

    def set_data(self, name, df, censor_on_index=True, censor_column=None):
        self.__data[name] = CensoredView(df, censor_on_index=censor_on_index, censor_column=censor_column)

    def add_data(self, name, censored_view):
        self.__data[name] = censored_view

    def set_date(self, dt):
        self.__dt = dt

    def get_data(self, name):
        return self.__data[name].get_censored(self.__dt)
