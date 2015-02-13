package com.bronto.ncsu.redis.queue;

/**
 * A work queue. Reliable implementations will keep track of which items have
 * been checked out for processing (dequeued), but not completed (released).
 * If work can not be completed the item may be requeued. Reliable
 * implementations implement a sweep operation to requeue any work which has
 * been checked out for a long time and may have been abandoned by a crashed
 * worker.
 *
 * See ReliableWorkQueue to work on your implementation.
 *
 * @param <T>
 */
public interface WorkQueue<T> {

  /**
   * Adds a new item to the end of the queue.
   *
   * @param item the item to enqueue
   */
  void enqueue(Item<T> item);

  /**
   * Dequeues the item at the head of the queue. If the queue is empty, returns null.
   *
   * <p><b>Enhancement:</b> <i>Implement pre-fetching. Dequeue items before
   * they are needed so the caller does not have to wait. Make sure items are
   * not pre-fetched too early, otherwise they be considered abandoned.</i></p>
   *
   * @return the dequeued item or null
   */
  Item<T> dequeue();

  /**
   * Releases the given item which is currently dequeued. This marks the item
   * as complete and removes it from the queue. If the item is not currently
   * dequeued do nothing. Once an item is removed it may not be requeued.
   *
   * @param item
   */
  void release(Item<T> item);

  /**
   * Requeues the given item which must be currently dequeued. The item is
   * requeued to the end of the queue. If the item is not currently dequeued
   * do nothing.
   *
   * @param item
   */
  void requeue(Item<T> item);

  /**
   * Requeue any items which have been dequeued for longer than the given
   * number of milliseconds.
   *
   * @param abandonedMillis
   */
  void sweep(long abandonedMillis);

  /**
   * Gets the name of the queue.
   *
   * @return
   */
  String getName();
}
