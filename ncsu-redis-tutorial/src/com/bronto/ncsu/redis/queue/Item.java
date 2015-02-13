package com.bronto.ncsu.redis.queue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * An item of work in a WorkQueue. Each item of work has a (unique) key and
 * user-provided value. The key will be used by the WorkQueue keep track of
 * the item in the queue. A WorkQueue may have more than one item with the
 * same value, but should never had more than one item with the same key.
 *
 * @param <T>
 */
public final class Item<T> {
  private final String key;
  private final T value;

  public Item(String key, T value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Item) {
      Item<?> other = (Item<?>) obj;
      return Objects.equal(other.key, key) &&
             Objects.equal(other.value, value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("value", value)
        .toString();
  }
}
