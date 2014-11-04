package com.linkedin.pinot.trace;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;


public class Spans implements Serializable {

  private static final long serialVersionUID = 2498745103550577576L;

  public static final long ROOT_SPAN_ID = 0;

  public static long genSpanId() {
    Random rand = new Random();
    long id = rand.nextLong();
    while (id == ROOT_SPAN_ID) {
      id = rand.nextLong();
    }
    return id;
  }

  public static String toString(Span span) {
    return String.format("{span: %s, getTraceId: %d, getParentId: %d, " +
            "getSpanId: %d, getDescription: %s, getStartTime: %s, getEndTime: %s}",
        span.getClass().getSimpleName(),
        span.getTraceId(),
        span.getParentId(),
        span.getSpanId(),
        span.getDescription(),
        new DateTime(span.getStartTime(TimeUnit.MILLISECONDS)),
        new DateTime(span.getEndTime(TimeUnit.MILLISECONDS)));
  }
}
