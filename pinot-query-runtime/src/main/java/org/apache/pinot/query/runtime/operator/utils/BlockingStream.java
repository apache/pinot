package org.apache.pinot.query.runtime.operator.utils;

public interface BlockingStream<E> {
  Object getId();

  E get();

  void cancel();
}
