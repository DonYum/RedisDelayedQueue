import redis
import logging
import datetime

logger = logging.getLogger(__name__)


# 延时队列
class RedisDelayedQueue():
    def __init__(self, redis_conn, que_name):
        self.redis_conn = redis_conn
        self.que_name = f'delayed_que-{que_name}'

    @property
    def now_ts(self):
        return datetime.datetime.now().timestamp()

    def add(self, val, expire=10):
        # redis不支持override，所以需要先删后建。
        self.del_val(val)
        score = self.now_ts + expire
        cnt = self.redis_conn.zadd(self.que_name, {val: score})
        logger.debug(f'[DelayedQueue]: add({val}), expire={expire}s, score={score}, cnt={cnt}')
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
