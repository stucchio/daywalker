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
