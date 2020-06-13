# RedisDelayedQueue

## Usage

使用方法：

- 生产：

```python
lazy_que = RedisDelayedQueue(redis_conn, 'prefix')
lazy_que.add('value1', 100)   # add()之后会在expire(秒)之后才能get到，重复add()的话重新计时。
```

- 消费：

```python
lazy_que = RedisDelayedQueue(redis_conn, 'prefix')
while True:
    res = lazy_que.pop()
    if res:
        process()
```

## 实现方式

使用redis的zset。

用释放时间点的时间戳做score，然后使用`zrangebyscore(que_name, 0, now_ts)`来获取计时结束的队列元素。
