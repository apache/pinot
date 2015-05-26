package com.linkedin.pinot.trace;

import java.io.IOException;


public class Traces {

  public static Span start(Trace trace, String msg) {
    return trace != null ? trace.start(msg) : EmptySpan.INSTANCE;
  }

  public static Span start(Trace trace, String msg, Span parent) {
    return (trace != null && parent != null) ?
        trace.start(msg, parent) : EmptySpan.INSTANCE;
  }

  public static Span end(Trace trace) {
    return trace != null ? trace.end() : EmptySpan.INSTANCE;
  }

  public static void close(Trace trace) throws IOException {
    if (trace != null) {
      trace.close();
    }
  }
}
