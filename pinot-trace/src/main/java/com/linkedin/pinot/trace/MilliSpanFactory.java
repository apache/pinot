package com.linkedin.pinot.trace;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


public class MilliSpanFactory implements SpanFactory, Serializable {

  private static final long serialVersionUID = -5450139279253472838L;

  @Override
  public Span newSpan(long traceId, String msg) {
    return new MilliSpan(traceId, Spans.genSpanId())
        .setStartTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .setDescription(msg);
  }

  @Override
  public Span newSpan(long traceId, String msg, long parentId) {
    return new MilliSpan(traceId, Spans.genSpanId())
        .setParentId(parentId)
        .setStartTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .setDescription(msg);
  }
}
