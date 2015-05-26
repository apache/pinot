package com.linkedin.pinot.trace;

import java.util.concurrent.TimeUnit;


public enum EmptySpan implements Span {
  INSTANCE;

  @Override
  public long getTraceId() {
    return 0;
  }

  @Override
  public long getSpanId() {
    return 0;
  }

  @Override
  public long getStartTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public Span setStartTime(long time, TimeUnit unit) {
    return this;
  }

  @Override
  public long getEndTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public Span setEndTime(long time, TimeUnit unit) {
    return this;
  }

  @Override
  public String getDescription() {
    return "EmptySpan";
  }

  @Override
  public Span setDescription(String description) {
    return this;
  }

  @Override
  public long getParentId() {
    return 0;
  }

  @Override
  public Span setParentId(long parentId) {
    return this;
  }
}
