import redis
import logging
import datetime

logger = logging.getLogger(__name__)


# 新版延时队列
# add()之后会在expire(秒)之后才能get到，重复add()的话重新计时。
# 使用方法：
#   生产：
#       lazy_que = RedisDelayedQueue(redis_conn, 'prefix')
#       lazy_que.add('value1', 100)
#   消费：
#       lazy_que = RedisDelayedQueue(redis_conn, 'prefix')
#       while True:
#           res = lazy_que.pop()
#           if res:
#               process()
class RedisDelayedQueue():
    def __init__(self, redis_conn, que_name):
        self.redis_conn = redis_conn
        self.que_name = f'delayed_que-{que_name}'

    @property
    def now_ts(self):
        return datetime.datetime.now().timestamp()

    def add(self, val, expire=10):
        # 先删后建
        self.del_val(val)
        score = self.now_ts + expire
        cnt = self.redis_conn.zadd(self.que_name, {val: score})
        logger.info(f'[DelayedQueue]: add({val}), expire={expire}s, score={score}, cnt={cnt}')
        return cnt

    def get_all(self):
        res = self.redis_conn.zrangebyscore(self.que_name, 0, self.now_ts)
        return res or None

    def pop(self):
        res = self.get_all() and self.redis_conn.zpopmin(self.que_name)
        res = res and res[0][0]
        return res or None

    def del_val(self, val):
        cnt = self.redis_conn.zrem(self.que_name, val)
        if cnt:
            logger.info(f'[DelayedQueue]: del_val({val})')
        return cnt

    def drop(self):
        cnt = self.redis_conn.delete(self.que_name)
        if cnt:
            logger.info(f'[DelayedQueue]: drop(), cnt={cnt}')
        return cnt


# 实现延时Queue
# add()之后会在expire(秒)之后才能get()到，重复add()的话重新计时。
# 使用方法：
#   生产：
#       lazy_que = RedisDelayedQueue(redis_conn, 'prefix', 100)
#       lazy_que.add('value1')
#   消费：
#       lazy_que = RedisDelayedQueue(redis_conn, 'prefix', 100)
#       while True:
#           res = lazy_que.get()
#           if res:
#               process()
class RedisDelayedQueueOld():
    def __init__(self, redis_conn, prefix, expire=10):
        self.redis_conn = redis_conn
        self.prefix = f'lazy_que:{prefix}'
        self.expire = expire

    def _key(self, val):
        return f'{self.prefix}-keys-{val}'

    def add(self, val):
        key = self._key(val)
        self.redis_conn.sadd(self.prefix, val)
        self.redis_conn.set(key, 1)
        self.redis_conn.expire(key, self.expire)
        logger.info(f'[DelayedQueue]: add({val})')

    def get(self):
        vals = self.redis_conn.smembers(self.prefix)
        for val in vals:
            key = self._key(val)
            res = self.redis_conn.get(key)
            if not res:
                return val

    def del_val(self, val):
        self.redis_conn.srem(self.prefix, val)
        self.redis_conn.delete(self._key(val))
        logger.info(f'[DelayedQueue]: del_val({val})')

    def drop(self):
        vals = self.redis_conn.smembers(self.prefix)
        for val in vals:
            res = self.redis_conn.delete(self._key(val))

        cnt = self.redis_conn.delete(self.prefix)
        if cnt:
            logger.info(f'[DelayedQueue]: drop(), cnt={cnt}')
        return cnt
