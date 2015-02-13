package com.bronto.ncsu.redis.tests;

import com.bronto.ncsu.redis.util.Utils;
import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.WorkQueue;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;

public class ReliabilityTest extends TestBase {

  // Lower this if the test is taking too long to run
  private static final int TEST_SIZE = 1000;

  private static final int NUM_PRODUCERS = 2;
  private static final int NUM_CONSUMERS = 2;
  private static final long ABANDONED_MILLIS = 2000;

  private static final Random RANDOM = new Random();

  private final String queueName = randomQueueName();
  private final List<String> values = Utils.randomByteStrings(16, TEST_SIZE);
  private final WorkerPool workerPool = new WorkerPool();

  private final Set<String> unconsumedValues = Sets.newConcurrentHashSet();
  private final AtomicInteger producerIdx = new AtomicInteger();

  @Test
  public void testReliability() throws Exception {
    // Mark all values as initially unconsumed
    unconsumedValues.addAll(values);

    // Start producers
    for (int i = 0; i < NUM_PRODUCERS; i++) {
      workerPool.execute(new Producer());
    }

    // Start consumers
    for (int i = 0; i < NUM_CONSUMERS; i++) {
      workerPool.execute(new Consumer());
    }

    Stopwatch emptyFor = Stopwatch.createStarted();

    // Wait for test to finish and periodically run sweeper. We consider
    // the test finished once all workers have completed or the queue has
    // appeared empty for some time.
    while (!workerPool.isDone() && emptyFor.elapsed(TimeUnit.MILLISECONDS) < ABANDONED_MILLIS + 1000) {
      // Sleep for 100ms
      Thread.sleep(100);

      // Check if the queue is empty. If it is not empty reset the empty timer.
      if (!isQueueEmpty()) {
        emptyFor.reset().start();
      }

      // Sweep any abandoned work
      sweep();
    }

    // Check for any worker errors
    workerPool.checkForErrors();

    if (!unconsumedValues.isEmpty()) {
      int itemsLost = unconsumedValues.size();
      double percentLost = 100.0 * itemsLost / values.size();
      String err = String.format("Expected no items to be lost. Actually lost %d items (%.2f%%).", itemsLost, percentLost);
      throw new AssertionError(err);
    }
  }

  private boolean isQueueEmpty() {
    return withConnection(new JedisOperation<Boolean>() {
      @Override
      public Boolean execute(Jedis conn) {
        return conn.keys("*" + queueName + "*").isEmpty();
      }
    });
  }

  private void sweep() {
    withQueue(queueName, new WorkQueueOperation<Void>() {
      @Override
      public Void execute(WorkQueue<String> workQueue) {
        try {
          workQueue.sweep(ABANDONED_MILLIS);
        } catch (UnsupportedOperationException e) {
          // May not be implemented. Just ignore this.
        }
        return null;
      }
    });
  }

  private class Producer extends WorkQueueRunnable {
    @Override
    protected String queueName() {
      return queueName;
    }

    @Override
    protected void runWithQueue(WorkQueue<String> queue) {
      int idx = producerIdx.getAndIncrement();
      while (idx < values.size()) {
        queue.enqueue(createItem(values.get(idx)));
        idx = producerIdx.getAndIncrement();
      }
    }
  }

  private class Consumer extends WorkQueueRunnable {
    @Override
    protected String queueName() {
      return queueName;
    }

    @Override
    protected void runWithQueue(WorkQueue<String> queue) {
      while (!unconsumedValues.isEmpty()) {
        Item<String> item = queue.dequeue();

        if (item == null) {
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          continue;
        }

        if (item.getValue() == null) {
          throw new NullPointerException("Dequeued item has null value!");
        }

        double rv = RANDOM.nextDouble();
        if (rv < 0.2) {
          queue.requeue(item);
        } else if (rv > 0.3) {
          unconsumedValues.remove(item.getValue());
          queue.release(item);
        }
      }
    }
  }
}
