package org.apache.pinot.spi.stream.buffer;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;


public class OnHeapMessageBatchBuffer implements MessageBatchBuffer<MessageBatch>, AutoCloseable {
  private final List<MessageBatch> buffer; // to store elements
  private final int capacity; // maximum number of elements in the buffer

  public OnHeapMessageBatchBuffer(int capacity) {
    this.capacity = capacity;
    this.buffer = new ArrayList<>(capacity);
  }

  @Override
  public synchronized void put(MessageBatch element) throws Exception {
    // Wait until the buffer has space
    while (buffer.size() == capacity) {
      wait();
    }
    buffer.add(element); // Add the new element to the buffer
    notifyAll(); // Notify all waiting threads that an element has been added
  }

  @Override
  public synchronized MessageBatch get() throws Exception {
    // Wait until the buffer is not empty
    while (buffer.isEmpty()) {
      wait();
    }
    MessageBatch element = buffer.remove(0); // Remove an element from the buffer
    notifyAll(); // Notify all waiting threads that an element has been removed
    return element;
  }

  // Additional utility methods

  @Override
  public int size() {
    return buffer.size();
  }

  @Override
  public boolean isEmpty() {
    return buffer.isEmpty();
  }

  @Override
  public boolean isFull() {
    return buffer.size() == capacity;
  }

  @Override
  public void close()
      throws Exception {
    buffer.clear();
  }
}
