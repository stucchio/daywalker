import sys
sys.path.append('.')

import unittest
import doctest
import daywalker.market as dw_market
import daywalker.broker as dw_broker
import daywalker.accounting as dw_accounting
import test.test_market as test_market

def load_tests(loader, tests, ignore):
    for module in [dw_market, dw_broker, dw_accounting]:
        tests.addTests(doctest.DocTestSuite(module))
    tests.addTests(doctest.DocFileSuite("../readme.md"))


    for all_test_suite in unittest.defaultTestLoader.discover('test/', pattern='test_*.py'):
        for test_suite in all_test_suite:
            tests.addTests(test_suite)

    return tests
