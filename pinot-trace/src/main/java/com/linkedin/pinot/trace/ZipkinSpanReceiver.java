package com.linkedin.pinot.trace;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.LogEntry;
import com.twitter.zipkin.gen.Scribe;
import com.twitter.zipkin.gen.zipkinCoreConstants;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class ZipkinSpanReceiver implements SpanReceiver, Serializable {

  private static final long serialVersionUID = -1891411264856834512L;

  private static final Logger logger = Logger.getLogger(ZipkinSpanReceiver.class);

  private static final int MAX_SPAN_BATCH_SIZE = 100;
  private static final int ZIPKIN_COLLECTOR_PORT = 9410;
  private static final String ZIPKIN_COLLECTOR_HOSTNAME = "localhost";

  private transient BlockingQueue<Span> queue;
  private transient ScheduledExecutorService service;
  private transient TProtocolFactory protocolFactory;
  private transient ByteArrayOutputStream buf;
  private transient TProtocol ioProtocol;
  private transient Endpoint endpoint;
  private transient Scribe.Client scribe = null;
  private transient volatile boolean started = false;

  private String serviceName;

  public ZipkinSpanReceiver(String serviceName) {
    this.serviceName = serviceName;
    internalInit();
  }

  private void internalInit() {
    queue = new ArrayBlockingQueue<Span>(256);
    service = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ZipkinSpanReceiver-%d").build());
    protocolFactory = new TBinaryProtocol.Factory();
    buf = new ByteArrayOutputStream();
    ioProtocol = protocolFactory.getProtocol(new TIOStreamTransport(buf));
  }

  @Override
  public void init() {
    if (!started) {

      try {
        String hostname = InetAddress.getLocalHost().getHostAddress();
        InetAddress address = InetAddress.getByName(hostname);
        int ipv4Address = ByteBuffer.wrap(address.getAddress()).getInt();
        endpoint = new Endpoint(ipv4Address, (short)0, serviceName);
      } catch (UnknownHostException e) {
        logger.error("ZipkinSpanReceiver init failed, due to unable to get the host name", e);
        return;
      }

      TTransport transport = new TFramedTransport(
          new TSocket(ZIPKIN_COLLECTOR_HOSTNAME, ZIPKIN_COLLECTOR_PORT));

      try {
        transport.open();
      } catch (TTransportException e) {
        logger.error("Failed to open thrift transport", e);
      }

      TProtocol protocol = protocolFactory.getProtocol(transport);
      this.scribe = new Scribe.Client(protocol);

      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          pushToZipkin(MAX_SPAN_BATCH_SIZE);
        }
      }, 0, 10, TimeUnit.SECONDS);
      started = true;
    }
  }

  com.twitter.zipkin.gen.Span toZipkinSpan(Span span) {
    com.twitter.zipkin.gen.Span zSpan = new com.twitter.zipkin.gen.Span()
        .setTrace_id(span.getTraceId())
        .setId(span.getSpanId())
        .setName(span.getDescription())
        .setAnnotations(toZipkinAnnotations(span, endpoint));
    if (span.getParentId() != Spans.ROOT_SPAN_ID) {
      zSpan.setParent_id(span.getParentId());
    }
    return zSpan;
  }

  List<Annotation> toZipkinAnnotations(Span span, Endpoint ep) {

    List<Annotation> result = new ArrayList<Annotation>();

    result.add(new Annotation()
        .setHost(ep)
        .setTimestamp(span.getStartTime(TimeUnit.MICROSECONDS))
        .setDuration(1)
        .setValue(zipkinCoreConstants.CLIENT_SEND));

    result.add(new Annotation()
        .setHost(ep)
        .setTimestamp(span.getStartTime(TimeUnit.MICROSECONDS))
        .setDuration(1)
        .setValue(zipkinCoreConstants.SERVER_RECV));

    result.add(new Annotation()
        .setHost(ep)
        .setTimestamp(span.getEndTime(TimeUnit.MICROSECONDS))
        .setDuration(1)
        .setValue(zipkinCoreConstants.SERVER_SEND));

    result.add(new Annotation()
        .setHost(ep)
        .setTimestamp(span.getEndTime(TimeUnit.MICROSECONDS))
        .setDuration(1)
        .setValue(zipkinCoreConstants.CLIENT_RECV));

    return result;
  }

  @Override
  public void receive(Span span) {
    this.queue.offer(span);
  }

  @Override
  public void close() throws IOException {
    service.shutdown();
    try {
      service.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("Failed to terminate zipkin receiver thread", e);
    }

    pushToZipkin(queue.size());

    scribe = null;
    started = false;
  }

  private void pushToZipkin(int maxNum) {
    logger.info("Pushing spans to zipkin...");
    List<Span> onFlightSpans = Lists.newArrayListWithCapacity(maxNum);
    queue.drainTo(onFlightSpans, maxNum);

    List<LogEntry> onFlightLogEntries = Lists.newArrayListWithCapacity(onFlightSpans.size());
    try {
      for (Span span : onFlightSpans) {
        buf.reset();
        com.twitter.zipkin.gen.Span zSpan = toZipkinSpan(span);
        zSpan.write(ioProtocol);
        LogEntry logEntry = new LogEntry(
            "zipkin", new String(Base64.encodeBase64(buf.toByteArray())));
        onFlightLogEntries.add(logEntry);
      }
    } catch (TException e) {
      logger.error("Failed to convert to zipkin span", e);
    }
    if (!onFlightLogEntries.isEmpty()) {
      try {
        scribe.Log(onFlightLogEntries);
      } catch (TException e) {
        logger.error("Failed to send spans to zipkin collector", e);
      }
    }
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(this.serviceName);
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    this.serviceName = (String)(ois.readObject());
    internalInit();
  }
}
