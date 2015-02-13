package com.bronto.ncsu.redis.tests;

import com.bronto.ncsu.redis.util.Utils;
import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.WorkQueue;

import com.google.common.base.Stopwatch;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DequeueLatencyTest extends TestBase {

  private static final int TEST_SIZE = 10000;

  @Test
  public void testDequeueLatency() throws Exception {
    WorkQueue<String> queue = createQueue(randomQueueName());
    List<String> values = Utils.randomByteStrings(16, TEST_SIZE);

    enqueueAll(queue, values);

    List<Long> timings = new ArrayList<Long>(TEST_SIZE);
    for (int i = 0; i < TEST_SIZE; i++) {
      Stopwatch sw = Stopwatch.createStarted();
      Item<String> item = queue.dequeue();
      sw.stop();
      timings.add(sw.elapsed(TimeUnit.NANOSECONDS));
      queue.release(item);
    }

    long totalTime = 0;
    for (long t : timings) {
      totalTime += t;
    }

    long avg = totalTime / timings.size();
    long residualSquaredSum = 0;
    for (long t : timings) {
      long residual = t - avg;
      residualSquaredSum += residual * residual;
    }
    double stddev = Math.sqrt(residualSquaredSum / timings.size());

    System.out.println(String.format("avg=%.3fµs, stddev=%.3fµs", avg / 1e3, stddev / 1e3));
  }
}
