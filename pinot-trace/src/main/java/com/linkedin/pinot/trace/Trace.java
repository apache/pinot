package com.linkedin.pinot.trace;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * Trace class, not thread safe
 */
public class Trace implements Closeable, Serializable {

  private static final Logger logger = Logger.getLogger(Trace.class);

  private transient List<SpanReceiver> receivers;
  private transient LinkedList<Span> spans;

  private SpanFactory factory;
  private long traceId;
  private long curSpanId;

  public Trace(SpanFactory factory) {
    this(factory, new Random().nextLong(), Spans.ROOT_SPAN_ID);
  }

  public Trace(SpanFactory factory, long traceId, long curSpanId) {
    this.traceId = traceId;
    this.factory = factory;
    this.curSpanId = curSpanId;
    init();
  }

  private void init() {
    this.receivers = Lists.newArrayList();
    this.spans = Lists.newLinkedList();
  }

  public Trace newTrace() {
    Trace trace = new Trace(factory, this.traceId, this.curSpanId);
    trace.receivers.addAll(this.receivers);
    return trace;
  }

  public void addReceiver(SpanReceiver receiver) {
    logger.info("Added receiver: " + receiver.getClass().getSimpleName());
    receiver.init();
    receivers.add(receiver);
  }

  public Span start(String msg) {
    return start(msg, curSpanId);
  }

  public Span start(String msg, Span parent) {
    return start(msg, parent.getSpanId());
  }

  private Span start(String msg, long parentId) {
    Span span = factory.newSpan(traceId, msg, parentId);
    curSpanId = span.getSpanId();
    spans.add(span);
    return span;
  }

  public Span end() {
    Span span = EmptySpan.INSTANCE;
    if (spans.size() > 0) {
      long time = System.currentTimeMillis();
      span = spans.getLast();
      span.setEndTime(time, TimeUnit.MILLISECONDS);
      spans.removeLast();

      if (spans.size() > 0) {
        curSpanId = spans.getLast().getSpanId();
      }

      for (SpanReceiver sr : receivers) {
        sr.receive(span);
      }
    }
    return span;
  }

  @Override
  public void close() throws IOException {
    for (int i=0; i < spans.size(); ++i) {
      end();
    }

    for (SpanReceiver receiver : receivers) {
      receiver.close();
      logger.info("Closed receiver: " + receiver.getClass().getSimpleName());
    }

    receivers.clear();
  }

  @Override
  public String toString() {
    return String.format("TraceId: %d, Current Span Id: %d, " +
            "Total spans: %d, Span Factory: %s, Total receivers: %d",
        traceId,
        curSpanId,
        spans.size(),
        factory.getClass().getSimpleName(),
        receivers.size());
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(this.factory);
    oos.writeObject(this.traceId);
    oos.writeObject(this.curSpanId);
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    this.factory = (SpanFactory)(ois.readObject());
    this.traceId = (Long)(ois.readObject());
    this.curSpanId = (Long)(ois.readObject());
    init();
  }
}
