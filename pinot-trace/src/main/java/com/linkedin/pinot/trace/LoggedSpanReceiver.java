package com.linkedin.pinot.trace;

import java.io.IOException;
import java.io.Serializable;
import org.apache.log4j.Logger;


public class LoggedSpanReceiver implements SpanReceiver, Serializable {

  private static final long serialVersionUID = -7704165561147681163L;

  private static final Logger logger = Logger.getLogger(LoggedSpanReceiver.class);

  @Override
  public void init() {
  }

  @Override
  public void receive(Span span) {
    logger.info("Receive span => " + Spans.toString(span));
  }

  @Override
  public void close() throws IOException {
  }
}
