package com.linkedin.pinot.trace;

import java.io.Closeable;
import java.io.Serializable;


/**
 * Receive a span
 */
public interface SpanReceiver extends Closeable, Serializable {

  /**
   * Do some init work
   * Note: init is supposed to be idempotent
   */
  void init();

  /**
   * Call whenever span is complete
   * @param span
   */
  void receive(Span span);
}
