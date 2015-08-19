package com.linkedin.thirdeye.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linkedin.thirdeye.impl.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kgopalak
 *
 */
public class MetricTimeSeries {
  private static final Logger LOGGER = LoggerFactory
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

  public MetricSchema getSchema()
  {
    return schema;
  }

  /**
   *
   * @param timeWindow
   * @param value
   */
  public void set(long timeWindow, String name, Number value) {
    initBufferForTimeWindow(timeWindow);
    ByteBuffer buffer = timeseries.get(timeWindow);
    buffer.position(schema.getOffset(name));
    MetricType metricType = schema.getMetricType(name);
    switch (metricType) {
    case SHORT:
      buffer.putShort(value.shortValue());
      break;
    case INT:
      buffer.putInt(value.intValue());
      break;
    case LONG:
      buffer.putLong(value.longValue());
      break;
    case FLOAT:
      buffer.putFloat(value.floatValue());
      break;
    case DOUBLE:
      buffer.putDouble(value.doubleValue());
      break;

    }
  }

  private void initBufferForTimeWindow(long timeWindow) {
    if (!timeseries.containsKey(timeWindow)) {
      byte[] bytes = new byte[schema.getRowSizeInBytes()];
      timeseries.put(timeWindow, ByteBuffer.wrap(bytes));
    }
  }

  public Number get(long timeWindow, String name) {
    ByteBuffer buffer = timeseries.get(timeWindow);
    Number ret = 0;

    if (buffer != null) {
      buffer = buffer.duplicate();
      MetricType metricType = schema.getMetricType(name);
      buffer.position(schema.getOffset(name));
      switch (metricType) {
      case SHORT:
        ret = buffer.getShort();
        break;
      case INT:
        ret = buffer.getInt();
        break;
      case LONG:
        ret = buffer.getLong();
        break;
      case FLOAT:
        ret = buffer.getFloat();
        break;
      case DOUBLE:
        ret = buffer.getDouble();
        break;
      }
    }
    return ret;
  }

  public void increment(long timeWindow, String name, Number delta) {
    initBufferForTimeWindow(timeWindow);
    ByteBuffer buffer = timeseries.get(timeWindow);
    Number newValue;

    if (buffer != null) {
      // get Old Value
      Number oldValue = get(timeWindow, name);
      MetricType metricType = schema.getMetricType(name);
      switch (metricType) {
      case SHORT:
        newValue = oldValue.shortValue() + delta.shortValue();
        break;
      case INT:
        newValue = oldValue.intValue() + delta.intValue();
        break;
      case LONG:
        newValue = oldValue.longValue() + delta.longValue();
        break;
      case FLOAT:
        newValue = oldValue.floatValue() + delta.floatValue();
        break;
      case DOUBLE:
        newValue = oldValue.doubleValue() + delta.doubleValue();
        break;
      default:
        throw new UnsupportedOperationException("unknown metricType:"
            + metricType + " for column:" + name);
      }
    } else {
      newValue = delta;
    }
    set(timeWindow, name, newValue);
  }

  public void aggregate(MetricTimeSeries series) {
    for (long timeWindow : series.timeseries.keySet()) {
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        String metricName = schema.getMetricName(i);
        Number delta = series.get(timeWindow, metricName);
        increment(timeWindow, metricName, delta);
      }
    }
  }

  /**
   * @param series
   *  A time series whose values should be reflected in this time series
   * @param timeRange
   *  Only include values from series that are in this time range
   */
  public void aggregate(MetricTimeSeries series, TimeRange timeRange)
  {
    for (long timeWindow : series.timeseries.keySet())
    {
      if (timeRange.contains(timeWindow))
      {
        for (int i = 0; i < schema.getNumMetrics(); i++)
        {
          String metricName = schema.getMetricName(i);
          Number delta = series.get(timeWindow, metricName);
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
    sb.append("(");
    for (long timeWindow : timeseries.keySet()) {
      sb.append("[");
      String delim = "";
      ByteBuffer buffer = timeseries.get(timeWindow);
      buffer.rewind();
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        if (i > 0) {
          delim = ",";
        }
        sb.append(delim).append(NumberUtils.readFromBuffer(buffer, schema.getMetricType(i)));
      }
      sb.append("]");
      sb.append("@");
      sb.append(timeWindow);
      sb.append(" ");
    }
    sb.setLength(sb.length() - 1);
    sb.append(")");
    return sb.toString();
  }

  public Number[] getMetricSums()
  {
    Number[] result = new Number[schema.getNumMetrics()];

    for (int i = 0; i < schema.getNumMetrics(); i++)
    {
      result[i] = 0;
    }

    for (Long time : timeseries.keySet())
    {
      for (int i = 0; i < schema.getNumMetrics(); i++)
      {
        String metricName = schema.getMetricName(i);
        MetricType metricType = schema.getMetricType(i);
        Number metricValue = get(time, metricName);

        switch (metricType)
        {
          case INT:
            result[i] = result[i].intValue() + metricValue.intValue();
            break;
          case SHORT:
            result[i] = result[i].shortValue() + metricValue.shortValue();
            break;
          case LONG:
            result[i] = result[i].longValue() + metricValue.longValue();
            break;
          case FLOAT:
            result[i] = result[i].floatValue() + metricValue.floatValue();
            break;
          case DOUBLE:
            result[i] = result[i].doubleValue() + metricValue.doubleValue();
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    return result;
  }

  @Override
  public int hashCode()
  {
    return timeseries.keySet().hashCode() + 13 * schema.hashCode();
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof MetricTimeSeries))
    {
      return false;
    }

    MetricTimeSeries ts = (MetricTimeSeries) o;

    return getTimeWindowSet().equals(ts.getTimeWindowSet()) && Arrays.equals(getMetricSums(), ts.getMetricSums());
  }
}
