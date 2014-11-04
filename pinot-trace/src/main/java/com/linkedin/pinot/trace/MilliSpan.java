package com.linkedin.pinot.trace;

import java.util.concurrent.TimeUnit;


public class MilliSpan implements Span {

  private long startTime;
  private long endTime;
  private long spanId;
  private long traceId;
  private long parentId;
  private String description;

  public MilliSpan(long traceId, long spanId) {
    this.traceId = traceId;
    this.spanId = spanId;
  }

  @Override
  public long getTraceId() {
    return this.traceId;
  }

  @Override
  public long getSpanId() {
    return this.spanId;
  }

  @Override
  public long getStartTime(TimeUnit unit) {
    return unit.convert(startTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public Span setStartTime(long time, TimeUnit unit) {
    this.startTime = unit.convert(time, TimeUnit.MILLISECONDS);
    return this;
  }

  @Override
  public long getEndTime(TimeUnit unit) {
    return unit.convert(endTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public Span setEndTime(long time, TimeUnit unit) {
    this.endTime = unit.convert(time, TimeUnit.MILLISECONDS);
    return this;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @Override
  public Span setDescription(String description) {
    this.description = description;
    return this;
  }

  @Override
  public long getParentId() {
    return this.parentId;
  }

  @Override
  public Span setParentId(long parentId) {
    this.parentId = parentId;
    return this;
  }
}
