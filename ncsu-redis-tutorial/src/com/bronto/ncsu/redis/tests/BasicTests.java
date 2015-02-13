package com.bronto.ncsu.redis.tests;

import static org.junit.Assert.assertEquals;

import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.WorkQueue;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

public class BasicTests extends TestBase {

  @Test
  public void testDequeueEmptyQueue() {
    WorkQueue<String> queue = createQueue(randomQueueName());
    assertEquals(null, queue.dequeue());
  }

  @Test
  public void testEnqueueDequeueRelease() {
    WorkQueue<String> queue = createQueue(randomQueueName());

    // Enqueue, dequeue, release
    queue.enqueue(createItem("foo001"));
    Item<String> item = queue.dequeue();
    assertEquals("foo001", item.getValue());
    queue.release(item);

    // Queue is empty so dequeue should return null
    assertEquals(null, queue.dequeue());
  }

  // Tests that the queue has FIFO semantics
  @Test
  public void testQueueIsFirstInFirstOut() {
    WorkQueue<String> queue = createQueue(randomQueueName());

    queue.enqueue(createItem("foo001"));
    queue.enqueue(createItem("foo002"));
    queue.enqueue(createItem("foo003"));

    Item<String> item1 = queue.dequeue();
    Item<String> item2 = queue.dequeue();
    Item<String> item3 = queue.dequeue();

    assertEquals("foo001", item1.getValue());
    assertEquals("foo002", item2.getValue());
    assertEquals("foo003", item3.getValue());

    queue.release(item1);
    queue.release(item2);
    queue.release(item3);

    // Queue is empty so dequeue should return null
    assertEquals(null, queue.dequeue());
  }

  // Tests that requeued items go to the end of the queue
  @Test
  public void testRequeueGoesToEnd() {
    WorkQueue<String> queue = createQueue(randomQueueName());

    queue.enqueue(createItem("foo001"));
    queue.enqueue(createItem("foo002"));

    // Dequeue the head of the queue (foo001) and requeue to the end
    queue.requeue(queue.dequeue());

    Item<String> item1 = queue.dequeue();
    Item<String> item2 = queue.dequeue();

    assertEquals("foo002", item1.getValue());
    assertEquals("foo001", item2.getValue());
  }

  @Test
  public void testSweep() throws Exception {
    WorkQueue<String> queue = createQueue(randomQueueName());
    Item<String> item1 = createItem("foo001");
    Item<String> item2 = createItem("foo002");
    Item<String> item3 = createItem("foo003");
    Item<String> item4 = createItem("foo004");

    queue.enqueue(item1);
    queue.enqueue(item2);
    queue.enqueue(item3);
    queue.enqueue(item4);

    // Dequeue two items (foo001 and foo002)
    queue.dequeue();
    queue.dequeue();

    // Wait 500 milliseconds
    Thread.sleep(500);

    // Dequeue an additional item. There are now three items in the working state.
    queue.dequeue();

    // Sweep the queue. This should requeue the items we originally dequeued
    // (foo001 and foo002), but leave foo003 in the working state.
    queue.sweep(500);

    // foo004 should definitely be at the head of the queue, but we can't actually
    // know the order that foo001 and foo002 will be requeued so we only ensure
    // they both are, not the order.
    assertEquals(item4, queue.dequeue());
    assertEquals(
        ImmutableSet.of(item1, item2),
        ImmutableSet.of(queue.dequeue(), queue.dequeue())
    );
  }
}
