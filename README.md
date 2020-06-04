# RedisDelayedQueue

## 思路1——RedisDelayedQueue

使用redis的zset。

用释放时间点的时间戳做score，然后使用`zrangebyscore(que_name, 0, now_ts)`来获取及时结束的值。

Ref：https://medium.com/@cheukfung/redis%E5%BB%B6%E8%BF%9F%E9%98%9F%E5%88%97-c940850a264f

## 思路2——RedisDelayedQueueOld

使用`expire`来维护时延信息。

## TODO

目前的版本不支持并发。

修改方法：使用RedLock做全局锁。
