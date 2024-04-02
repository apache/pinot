package org.apache.pinot.spi.stream.buffer;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;


public class OnHeapMessageBatchBuffer implements MessageBatchBuffer<MessageBatch>, AutoCloseable {
  private final List<MessageBatch> _buffer; // to store elements
  private final int _capacity; // maximum number of elements in the buffer

  private volatile boolean _closed = false;

  public OnHeapMessageBatchBuffer(int capacity) {
    this._capacity = capacity;
    this._buffer = new ArrayList<>(capacity);
  }

  @Override
  public synchronized void put(MessageBatch element) throws Exception {
    // Wait until the buffer has space
    while (_buffer.size() == _capacity && !_closed) {
      wait();
    }

    if (_closed) {
      throw new IllegalStateException("Cannot put elements to a closed buffer");
    }

    _buffer.add(element); // Add the new element to the buffer
    notifyAll(); // Notify all waiting threads that an element has been added
  }

  @Override
  public synchronized MessageBatch get() throws Exception {
    // Wait until the buffer is not empty
    while (_buffer.isEmpty() && !_closed) {
      wait();
    }

    if (_closed) {
      throw new IllegalStateException("Cannot get elements from a closed buffer");
    }

    MessageBatch element = _buffer.remove(0); // Remove an element from the buffer
    notifyAll(); // Notify all waiting threads that an element has been removed
    return element;
  }

  // Additional utility methods

  @Override
  public int size() {
    return _buffer.size();
  }

  @Override
  public boolean isEmpty() {
    return _buffer.isEmpty();
  }

  @Override
  public boolean isFull() {
    return _buffer.size() == _capacity;
  }

  @Override
  public void close()
      throws Exception {
    //TODO: Clearing the buffer would lead to data loss. Need to handle this case.
    _buffer.clear();
    _closed = true;
  }
}
