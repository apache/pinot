package com.linkedin.thirdeye.bootstrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kgopalak
 * 
 */
public class MetricTimeSeries {
  private static final Logger LOG = LoggerFactory
      .getLogger(MetricTimeSeries.class);

  Map<Long, ByteBuffer> timeseries;

  private MetricSchema schema;

  /**
   * 
   * @param schema
   */
  public MetricTimeSeries(MetricSchema schema) {
    timeseries = new HashMap<Long, ByteBuffer>();
    this.schema = schema;
  }

  /**
   * 
   * @param timeWindow
   * @param index
   * @param value
   */
  public void set(long timeWindow, String name, int value) {
    if (!timeseries.containsKey(timeWindow)) {
      timeseries.put(timeWindow,
          ByteBuffer.allocate(schema.getRowSizeInBytes()));
    }
    ByteBuffer buffer = timeseries.get(timeWindow);
    buffer.position(schema.getOffset(name));
    buffer.putInt(value);
  }

  public int get(long timeWindow, String name) {
    ByteBuffer buffer = timeseries.get(timeWindow);
    if (buffer != null) {
      // TODO:handle other data types
      buffer.position(schema.getOffset(name));
      return buffer.getInt();
    } else {
      return 0;
    }
  }

  public void increment(long timeWindow, String name, int delta) {
    ByteBuffer buffer = timeseries.get(timeWindow);
    if (buffer != null) {
      // TODO:handle other data types
      buffer.position(schema.getOffset(name));
      int curValue = buffer.getInt();
      buffer.position(schema.getOffset(name));
      buffer.putInt(curValue + delta);
    }
  }

  public void aggregate(MetricTimeSeries series) {
    for (long timeWindow : series.timeseries.keySet()) {
      ByteBuffer byteBuffer = series.timeseries.get(timeWindow);
      if (!timeseries.containsKey(timeWindow)) {
        timeseries.put(timeWindow, byteBuffer);
      } else {
        for (int i = 0; i < schema.getNumMetrics(); i++) {
          // TODO: handle other data types
          String metricName = schema.getMetricName(i);
          int delta = get(timeWindow, metricName);
          increment(timeWindow, metricName, delta);
        }
      }
    }
  }

  public static MetricTimeSeries fromBytes(byte[] buf, MetricSchema schema)
      throws IOException {
    MetricTimeSeries series = new MetricTimeSeries(schema);
    DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
    int numTimeWindows = in.readInt();
    int bufferSize = in.readInt();
    for (int i = 0; i < numTimeWindows; i++) {
      long timeWindow = in.readLong();
      byte[] bytes = new byte[bufferSize];
      in.readFully(bytes);
      series.timeseries.put(timeWindow, ByteBuffer.wrap(bytes));
    }
    return series;
  }

  /**
   * 
   * @return
   */
  public Set<Long> getTimeWindowSet() {
    return timeseries.keySet();
  }

  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    // write the number of timeWindows
    out.writeInt(timeseries.size());
    // write the size of the metric buffer for each timeWindow
    out.writeInt(schema.getRowSizeInBytes());
    for (long time : timeseries.keySet()) {
      out.writeLong(time);
      out.write(timeseries.get(time).array());
    }
    return baos.toByteArray();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (long timeWindow : timeseries.keySet()) {
      sb.append(timeWindow);
      sb.append(":[");
      String delim = "";
      ByteBuffer buffer = timeseries.get(timeWindow);
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        sb.append(buffer.getInt()).append(delim);
        delim = ",";
      }
      sb.append("]\n");
    }
    return sb.toString();
  }
}
