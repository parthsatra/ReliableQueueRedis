package com.bronto.ncsu.redis.queue;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * This class is the starting point for your reliable queue implementation. The
 * initial implementation is a naive non-reliable queue.
 */
public class ReliableWorkQueue implements WorkQueue<String> {
  private final Jedis conn;
  private final String queueName;

  public ReliableWorkQueue(Jedis conn, String queueName) {
    this.conn = conn;
    this.queueName = queueName;
  }

  @Override
  public void enqueue(Item<String> item) {
    conn.lpush(queueName, item.getKey());
    conn.hset(queueName+"values", item.getKey(), item.getValue());
  }

  @Override
  public Item<String> dequeue() {
    String key = conn.rpop(queueName);
    if (key == null) {
      return null;
    }
    conn.zadd(queueName+"working", System.currentTimeMillis(), key);
    String data = conn.hget(queueName+"values", key);
    return new Item<String>(key, data);
  }

  @Override
  public void release(Item<String> item) {
    conn.zrem(queueName+"working", item.getKey());
    conn.hdel(queueName+"values", item.getKey());
  }

  @Override
  public void requeue(Item<String> item) {
    long count = conn.zrem(queueName+"working", item.getKey());
    if(count == 1) {
      conn.lpush(queueName, item.getKey());
    }
  }

  @Override
  public void sweep(long abandonedMillis) {
    Set<String> set = conn.zrangeByScore(queueName+"working", 0, (System.currentTimeMillis() - abandonedMillis));
    for(String key : set) {
      conn.lpush(queueName, key);
      conn.zrem(queueName + "working", key);
    }
  }

  @Override
  public String getName() {
    return queueName;
  }
}
