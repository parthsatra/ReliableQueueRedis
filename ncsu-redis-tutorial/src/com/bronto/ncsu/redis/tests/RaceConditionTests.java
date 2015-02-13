package com.bronto.ncsu.redis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.WorkQueue;

import com.google.common.base.Throwables;

import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

public class RaceConditionTests extends TestBase {

  public void testDoubleRequeueRaceHelper(WorkQueue<String> queue1, WorkQueue<String> queue2) throws Exception {
    queue1.enqueue(createItem("foo001"));
    final Item<String> item = queue1.dequeue();
    final CyclicBarrier barrier = new CyclicBarrier(2);

    // Submit for the item to be requeued twice concurrently
    Future<?> f1 = ex.submit(new Requeuer(queue1, item, barrier));
    Future<?> f2 = ex.submit(new Requeuer(queue2, item, barrier));

    // Wait for the requeues to complete
    f1.get();
    f2.get();

    // The item should have been requeued only once. One of the attempts
    // to requeue should have been ignored.
    assertEquals(item, queue1.dequeue());
    assertNull(queue1.dequeue());

    // Release the dequeued item
    queue1.release(item);
  }

  @Test
  public void testDoubleRequeueRace() throws Exception {
    WorkQueue<String> queue1 = createQueue(randomQueueName());
    WorkQueue<String> queue2 = createQueue(queue1.getName());

    // Run the test 1000 times to reduce the chance the test passes by chance
    for (int i = 0; i < 1000; i++) {
      testDoubleRequeueRaceHelper(queue1, queue2);
    }
  }

  private class Requeuer implements Runnable {
    private final WorkQueue<String> queue;
    private final Item<String> item;
    private final CyclicBarrier barrier;

    private Requeuer(WorkQueue<String> queue, Item<String> item, CyclicBarrier barrier) {
      this.queue = queue;
      this.item = item;
      this.barrier = barrier;
    }

    @Override
    public void run() {
      try {
        barrier.await();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }

      queue.requeue(item);
    }
  }
}
