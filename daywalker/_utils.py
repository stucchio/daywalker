import pandas as pd


def chunks(iterator, chunk_size):
    """
    Takes an iterator or collection and breaks it up into chunks of fixed size.
    """
    chunk = []
    for x in iterator:
        chunk.append(x)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk

class HasDfDict:
    META_FIELDS = []

    def df_dict(self):
        d = {}
        for c in self.DICT_COLUMNS:
            d[c] = getattr(self, c)
        for (field, prefix) in self.META_FIELDS:
            meta = getattr(self, field)
            for k in meta:
                d[prefix + k] = meta[k]
        return d


class DictableToDataframe:
    def __init__(self):
        self.buffer = []
        self.df_result = pd.DataFrame()

    def append(self, o):
        if isinstance(o, dict):
            self.buffer.append(o)
        else:
            self.buffer.append(o.df_dict())

    def append_dict(self, o):
        self.buffer.append(o)

    def get(self):
        if len(self.buffer) == 0:  # Easy case
            return self.df_result

        result = [o for o in self.buffer]
        self.buffer = []
        if len(self.df_result) == 0:
            self.df_result = pd.DataFrame(result)
        else:
            self.df_result = pd.concat([self.df_result, pd.DataFrame(result)])
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
