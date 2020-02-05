import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "daywalker",
    version = "0.0.1",
    author = "Chris Stucchio",
    author_email = "hi@chrisstucchio.com",
    description = ("A pandas-ish stock market backtesting framework for python."),
    license = "GPL 3.0",
    keywords = "stock_market backtesting trading",
    url = "http://github.com/stucchio/daywalker",
    packages=['daywalker'],
    long_description=read('readme.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Backtesting",
        "License :: OSI Approved :: GPL License",
    ],
)
