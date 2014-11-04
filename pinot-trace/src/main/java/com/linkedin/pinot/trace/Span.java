package com.linkedin.pinot.trace;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


public interface Span extends Serializable {

  long getTraceId();

  long getSpanId();

  long getStartTime(TimeUnit unit);

  Span setStartTime(long time, TimeUnit unit);

  long getEndTime(TimeUnit unit);

  Span setEndTime(long time, TimeUnit unit);

  String getDescription();

  Span setDescription(String description);

  long getParentId();

  Span setParentId(long parentId);
}
