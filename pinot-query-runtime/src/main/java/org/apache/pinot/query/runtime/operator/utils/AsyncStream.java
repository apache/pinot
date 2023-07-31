package org.apache.pinot.query.runtime.operator.utils;

import javax.annotation.Nullable;


public interface AsyncStream<E> {
  Object getId();

  @Nullable
  E poll();

  void addOnNewDataListener(OnNewData onNewData);

  void cancel();

  interface OnNewData {
    void newDataAvailable();
  }
}
