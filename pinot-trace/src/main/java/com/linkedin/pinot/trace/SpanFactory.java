package com.linkedin.pinot.trace;

import java.io.Serializable;


public interface SpanFactory extends Serializable {

  Span newSpan(long traceId, String msg);

  Span newSpan(long traceId, String msg, long parentId);
}
