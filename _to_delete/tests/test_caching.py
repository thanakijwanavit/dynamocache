import unittest
from unittest.mock import patch
from src.dynamocache.dynamocache import Dynamocache
import time, pytest


class TestDynamocache(unittest.TestCase):
    def test_memoize(self):
        dynamocache = Dynamocache('test-cache')
        memoize_decorator = dynamocache.memoize(ttl_seconds=5)

        @memoize_decorator
        def slow_function():
            return 42

        # Call the function twice to check caching behavior
        self.assertEqual(slow_function(), 42)
        self.assertEqual(slow_function(), 42)

    def test_create_table(self):
        dynamocache = Dynamocache('test-cache')
        dynamocache.create_table()
    
    @pytest.mark.timeout(3)
    def test_timing(self):
        dynamocache = Dynamocache('test-cache')
        memoize_decorator = dynamocache.memoize(ttl_seconds=5)
        @memoize_decorator
        def slow_function():
            time.sleep(1)
            return 42

        for _ in range(10):
            slow_function()
        


if __name__=='__main__':
    unittest.main()
    # TestDynamocache().test_create_table()