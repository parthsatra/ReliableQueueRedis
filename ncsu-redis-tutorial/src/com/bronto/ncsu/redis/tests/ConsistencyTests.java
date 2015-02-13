package com.bronto.ncsu.redis.tests;

import com.bronto.ncsu.redis.queue.Item;
import com.bronto.ncsu.redis.queue.WorkQueue;

import org.junit.Assert;
import org.junit.Test;

public class ConsistencyTests extends TestBase {
  @Test
  public void testDoubleRequeue() throws Exception {
    WorkQueue<String> queue = createQueue(randomQueueName());
    queue.enqueue(createItem("foo001"));

    Item<String> item = queue.dequeue();

    // The second requeue should do nothing since the item will no longer be in the working state.
    queue.requeue(item);
    queue.requeue(item);

    // Dequeue and release the only item in the queue.
    queue.release(queue.dequeue());

    // Check that the queue is empty by testing that dequeue returns null.
    Assert.assertNull("Item requeued twice into queue.", queue.dequeue());
  }

  @Test
  public void testReleaseRequeue() throws Exception {
    WorkQueue<String> queue = createQueue(randomQueueName());
    queue.enqueue(createItem("foo001"));

    Item<String> item = queue.dequeue();
    queue.release(item);

    // This request should be ignored since the item has been released.
    queue.requeue(item);

    // Check that the queue is empty by testing that dequeue returns null.
    Assert.assertNull("Released items should not be requeued.", queue.dequeue());
  }
}
