import time
import functools
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, BinaryAttribute
from pynamodb.exceptions import TableError
import pickle


class Dynamocache:
    def __init__(self, table_name, region_name='ap-southeast-1', access_key=None, secret_key=None, session_token=None):
        self.table_name = table_name
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token

    def create_table(self):
        try:
            class CacheModel(Model):
                class Meta:
                    region = self.region_name
                    table_name = self.table_name
                    aws_access_key_id = self.access_key
                    aws_secret_access_key = self.secret_key
                    aws_session_token = self.session_token

                key = UnicodeAttribute(hash_key=True)
            CacheModel.create_table(wait=True)
        except TableError as e:
            print("error creating table",e)
            pass

    def memoize(self, ttl_seconds=60):
        class CacheModel(Model):
            class Meta:
                region = self.region_name
                table_name = self.table_name
                aws_access_key_id = self.access_key
                aws_secret_access_key = self.secret_key
                aws_session_token = self.session_token

            key = UnicodeAttribute(hash_key=True)
            value = BinaryAttribute(legacy_encoding=False)
            ttl = NumberAttribute()

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Compute the cache key from the function name and arguments
                key = func.__name__ + repr(args) + repr(kwargs)
                print('key is ', key)
                # Check if the result is cached in DynamoDB
                cache_item = next(CacheModel.query(key),None)
                if cache_item and cache_item.ttl > int(time.time()):
                    # If the cached result is still valid, return it
                    data = cache_item.value
                    return pickle.loads(data)
                else:
                    # If the cached result is invalid or missing, compute the result
                    result = func(*args, **kwargs)
                    resultData = pickle.dumps(result)
                    # Store the result in DynamoDB with a TTL
                    cache_item = CacheModel(key=key, value=resultData, ttl=int(time.time()) + ttl_seconds)
                    cache_item.save()
                    # Return the result
                    return result
            return wrapper
        return decorator




if __name__ == '__main__':
    global r
    r = 0
    table = Dynamocache('test-cache')

    @table.memoize(5)
    def test_func():
        print('function is run, this should only run once')
        global r
        r += 1
        return 1
    test_func()
    test_func()

    assert r == 1

