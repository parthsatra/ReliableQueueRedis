package com.bronto.ncsu.redis.tests;

import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.ReliableWorkQueue;
import com.bronto.ncsu.redis.util.Utils;
import com.bronto.ncsu.redis.queue.WorkQueue;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public abstract class TestBase {

  private static final String REDIS_HOST = "10.139.64.244";
  private static final int REDIS_PORT = 6379;
  private static final int REDIS_DB = 0;

  private static final int KEY_SIZE = 16;

  protected ExecutorService ex;
  private JedisPool jedisPool;

  @Before
  public void setUp() {
    ex = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("RedisTestWorker-%d")
            .setDaemon(true)
            .build());

    jedisPool = new JedisPool(
        new JedisPoolConfig(), REDIS_HOST, REDIS_PORT, 2000, null, REDIS_DB, null
    );
  }

  @After
  public void tearDown() {
    ex.shutdownNow();
    jedisPool.close();
  }

  protected final String randomQueueName() {
    return "queue-" + Utils.randomBytesString(16);
  }

  protected final WorkQueue<String> createQueue(String name) {
    return createQueue(jedisPool.getResource(), name);
  }

  private WorkQueue<String> createQueue(Jedis conn, String name) {
    return new ReliableWorkQueue(conn, name);
  }

  private static <T> T withConnection(JedisPool pool, TestBase.JedisOperation<T> operation) {
    Jedis conn = pool.getResource();
    try {
      return operation.execute(conn);
    } finally {
      conn.close();
    }
  }

  protected final <T> T withConnection(JedisOperation<T> operation) {
    return withConnection(jedisPool, operation);
  }

  protected final <T> T withQueue(final String queueName, final WorkQueueOperation<T> operation) {
    return withConnection(new JedisOperation<T>() {
      @Override
      public T execute(Jedis conn) {
        return operation.execute(createQueue(conn, queueName));
      }
    });
  }

  protected final void enqueueAll(WorkQueue<String> workQueue, Iterable<String> values) {
    for (String value : values) {
      workQueue.enqueue(createItem(value));
    }
  }

  protected static <T> Item<T> createItem(T value) {
    return new Item<T>(randomKey(), value);
  }

  protected static String randomKey() {
    return Utils.randomBytesString(KEY_SIZE);
  }

  protected class WorkerPool {
    private final List<Future<?>> workerFutures = new ArrayList<Future<?>>();

    public void execute(Runnable worker) {
      workerFutures.add(ex.submit(worker));
    }

    public boolean isDone() {
      for (Future<?> workerFuture : workerFutures) {
        if (!workerFuture.isDone()) {
          return false;
        }
      }

      return true;
    }

    public void checkForErrors() throws RuntimeException {
      boolean interrupted = false;

      try {
        for (Future<?> workerFuture : workerFutures) {
          if (workerFuture.isDone()) {
            try {
              workerFuture.get();
            } catch (ExecutionException e) {
              throw Throwables.propagate(e.getCause());
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        }
      } finally {
        // Reset interrupted state if we suppressed it previously
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  protected abstract class WorkQueueRunnable implements Runnable {

    protected abstract String queueName();
    protected abstract void runWithQueue(WorkQueue<String> queue);

    @Override
    public void run() {
      withQueue(queueName(), new WorkQueueOperation<Void>() {
        @Override
        public Void execute(WorkQueue<String> workQueue) {
          runWithQueue(workQueue);
          return null;
        }
      });
    }
  }

  protected interface WorkQueueOperation<T> {
    T execute(WorkQueue<String> workQueue);
  }

  protected interface JedisOperation<T> {
    T execute(Jedis conn);
  }
}
