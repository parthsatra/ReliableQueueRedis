package com.bronto.ncsu.redis.queue;

import com.bronto.ncsu.redis.tests.BasicTests;
import com.bronto.ncsu.redis.tests.ConsistencyTests;
import com.bronto.ncsu.redis.tests.DequeueLatencyTest;
import com.bronto.ncsu.redis.tests.RaceConditionTests;
import com.bronto.ncsu.redis.tests.ReliabilityTest;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * Run this to test your ReliableWorkQueue.
 */
public class RunTests {
  public static void main(String[] args) throws Exception {
    JUnitCore junit = new JUnitCore();
    junit.addListener(new TextListener(System.out));

    System.out.println("Running unit tests...");
    Result result = junit.run(
        BasicTests.class,
        ReliabilityTest.class,
        ConsistencyTests.class,
        RaceConditionTests.class
    );

    if (!result.wasSuccessful()) {
      System.exit(1);
    }

    System.out.println();
    System.out.println("Running dequeue latency test...");
    JUnitCore.runClasses(DequeueLatencyTest.class);
  }
}
