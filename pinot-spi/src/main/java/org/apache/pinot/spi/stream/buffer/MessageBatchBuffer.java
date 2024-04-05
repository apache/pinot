package org.apache.pinot.spi.stream.buffer;

public interface MessageBatchBuffer<T> extends AutoCloseable {
  // Adds an item to the buffer. If the buffer is full, this method should block until space becomes available.
  void put(T item) throws Exception;

  // Removes and returns an item from the buffer. If the buffer is empty, this method should block until an item becomes available.
  T get() throws Exception;

  // Returns the number of items in the buffer.
  int size();

  // Returns true if the buffer is empty, false otherwise.
  boolean isEmpty();

// Returns true if the buffer is full, false otherwise.
  boolean isFull();
}
