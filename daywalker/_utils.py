import pandas as pd

class DictableToDataframe:
    def __init__(self):
        self.buffer = []
        self.df_result = pd.DataFrame()

    def append(self, o):
        self.buffer.append(o)

    def get(self):
        result = []
        for o in self.buffer:
            result.append(o.df_dict())
        if len(result) == 0:
            return self.df_result
        if len(self.df_result) == 0:
            self.df_result = pd.DataFrame(result)
        else:
            self.df_result = pd.concat(self.df_result, self.df_result)
        self.buffer = []
        return self.df_result

class DataframeBuffer:
    def __init__(self):
        self.buffer = []
        self.df = None

    def append(self, df):
        self.buffer.append(df)

    def get(self):
        if (len(self.buffer) == 0) and (self.df is None):
            return pd.DataFrame()
        if self.df is None:
            self.df = pd.concat(self.buffer)
            self.buffer = []
        else:
            self.df = pd.concat([self.df] + self.buffer)
            self.buffer = []
        return self.df
