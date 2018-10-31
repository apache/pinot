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
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.util.NumberUtils;

/**
 * @author kgopalak
 */
public class MetricTimeSeries {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTimeSeries.class);
  private static final String NULL_NUMBER_TOSTRING_STRING = "null";

  // Mapping from timestamp to the value of metrics. (One value per metric and multiple metrics per timestamp.)
  private Map<Long, ByteBuffer> metricsValue;

  private Map<Long, boolean[]> hasValue;

  private MetricSchema schema;

  /**
   * @param schema
   */
  public MetricTimeSeries(MetricSchema schema) {
    metricsValue = new HashMap<>();
    hasValue = new HashMap<>();
    this.schema = schema;
  }

  public MetricSchema getSchema() {
    return schema;
  }

  /**
   * Sets the value of the metric with the given timestamp.
   *
   * @param timeWindow the timestamp for the value.
   * @param name metric name.
   * @param value a non-null value.
   */
  public void set(long timeWindow, String name, Number value) {
    initBufferForTimeWindow(timeWindow);
    setMetricValue(timeWindow, name, value);
    setHasValue(timeWindow, name);
  }

  private void setMetricValue(long timeWindow, String name, Number value) {
    ByteBuffer buffer = metricsValue.get(timeWindow);
    buffer.position(schema.getOffset(name));
    MetricType metricType = schema.getMetricType(name);
    NumberUtils.addToBuffer(buffer, value, metricType);
  }

  private void setHasValue(long timeWindow, String name) {
    boolean[] buffer = hasValue.get(timeWindow);
    buffer[schema.getMetricIndex(name)] = true;
  }

  private void initBufferForTimeWindow(long timeWindow) {
    if (!metricsValue.containsKey(timeWindow)) {
      byte[] metricsValueBytes = new byte[schema.getRowSizeInBytes()];
      metricsValue.put(timeWindow, ByteBuffer.wrap(metricsValueBytes));
      boolean[] hasValueBytes = new boolean[schema.getNumMetrics()];
      hasValue.put(timeWindow, hasValueBytes);
    }
  }

  /**
   * Gets the metric value with the timestamp if the value exists; otherwise, null is returned.
   *
   * @param timeWindow the timestamp.
   * @param name the metric name.
   *
   * @return the metric value if exists; otherwise, null is returned.
   */
  public Number get(long timeWindow, String name) {
    return getOrDefault(timeWindow, name, null);
  }

  /**
   * Gets the metric value with the timestamp if the value exists; otherwise, the default number is returned.
   *
   * @param timeWindow the timestamp.
   * @param name the metric name.
   * @param defaultNumber the default number for the returned value if the target value does not exist.
   *
   * @return the metric value with the timestamp if the value exists; otherwise, the default number is returned.
   */
  public Number getOrDefault(long timeWindow, String name, Number defaultNumber) {
    Number ret = defaultNumber;

    boolean[] hasValueBuffer = hasValue.get(timeWindow);
    if (hasValueBuffer != null && hasValueBuffer[schema.getMetricIndex(name)]) {
      ByteBuffer buffer = metricsValue.get(timeWindow);
      if (buffer != null) {
        buffer = buffer.duplicate();
        buffer.position(schema.getOffset(name));
        MetricType metricType = schema.getMetricType(name);
        ret = NumberUtils.readFromBuffer(buffer, metricType);
      }
    }

    return ret;
  }

  /**
   * Increments the metric value of the timestamp with delta.
   *
   * @param timeWindow the timestamp.
   * @param name the metric name.
   * @param delta the non-null value to be added to the metric value.
   */
  public void increment(long timeWindow, String name, Number delta) {
    Number newValue = delta;
    Number oldValue = getOrDefault(timeWindow, name, null);

    if (oldValue != null) {
      MetricType metricType = schema.getMetricType(name);
      newValue = NumberUtils.sum(newValue, oldValue, metricType);
      if (newValue == null) {
        throw new UnsupportedOperationException(
            "unknown metricType:" + metricType + " for column:" + name);
      }
    }

    set(timeWindow, name, newValue);
  }

  public void aggregate(MetricTimeSeries series) {
    for (long timeWindow : series.metricsValue.keySet()) {
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        String metricName = schema.getMetricName(i);
        Number delta = series.getOrDefault(timeWindow, metricName, null);
        if (delta != null) {
          increment(timeWindow, metricName, delta);
        }
      }
    }
  }

  /**
   * @param series
   *          A time series whose values should be reflected in this time series
   * @param timeRange
   *          Only include values from series that are in this time range
   */
  public void aggregate(MetricTimeSeries series, TimeRange timeRange) {
    for (long timeWindow : series.metricsValue.keySet()) {
      if (timeRange.contains(timeWindow)) {
        for (int i = 0; i < schema.getNumMetrics(); i++) {
          String metricName = schema.getMetricName(i);
          Number delta = series.getOrDefault(timeWindow, metricName, null);
          if (delta != null) {
            increment(timeWindow, metricName, delta);
          }
        }
      }
    }
  }

  // TODO: Consider default null value; before that, this method is set to private
  private static MetricTimeSeries fromBytes(byte[] buf, MetricSchema schema) throws IOException {
    MetricTimeSeries series = new MetricTimeSeries(schema);
    DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
    int numTimeWindows = in.readInt();
    int bufferSize = in.readInt();
    for (int i = 0; i < numTimeWindows; i++) {
      long timeWindow = in.readLong();
      byte[] bytes = new byte[bufferSize];
      in.readFully(bytes);
      series.metricsValue.put(timeWindow, ByteBuffer.wrap(bytes));
      boolean[] hasValues = new boolean[schema.getNumMetrics()];
      for (int numMetrics = 0; numMetrics < schema.getNumMetrics(); numMetrics++) {
        hasValues[numMetrics] = true;
      }
      series.hasValue.put(timeWindow, hasValues);
    }
    return series;
  }

  /**
   * @return
   */
  public Set<Long> getTimeWindowSet() {
    return metricsValue.keySet();
  }

  // TODO: Consider default null value; before that, this method is set to private
  private byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    // write the number of timeWindows
    out.writeInt(metricsValue.size());
    // write the size of the metric buffer for each timeWindow
    out.writeInt(schema.getRowSizeInBytes());
    for (long time : metricsValue.keySet()) {
      out.writeLong(time);
      out.write(metricsValue.get(time).array());
    }
    return baos.toByteArray();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (long timeWindow : metricsValue.keySet()) {
      sb.append("[");
      String delim = "";
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        if (i > 0) {
          delim = ",";
        }
        sb.append(delim);
        Number number = getOrDefault(timeWindow, schema.getMetricName(i), null);
        if (number != null) {
          sb.append(number);
        } else {
          // TODO: read user specified null value from schema
          sb.append(NULL_NUMBER_TOSTRING_STRING);
        }
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

  public Number[] getMetricSums() {
    Number[] sum = new Number[schema.getNumMetrics()];
    int[] bucketCount = new int[schema.getNumMetrics()];
    calculateMetricSumAndBucketCount(sum, bucketCount);

    return sum;
  }

  /**
   * Returns the average values of metrics. If a metric does not have any values, then its average value is null.
   *
   * @return the average values of metrics.
   */
  public Double[] getMetricAvgs() {
    return getMetricAvgs(null);
  }

  /**
   * Returns the average values of metrics. If a metric does not have any values, then its average value is
   * valueOfdividedByZero.
   *
   * @param valueOfdividedByZero the value to be used when a metric does not have any values.
   *
   * @return the average values of metrics.
   */
  public Double[] getMetricAvgs(Double valueOfdividedByZero) {
    Number[] sum = new Number[schema.getNumMetrics()];
    int[] bucketCount = new int[schema.getNumMetrics()];
    calculateMetricSumAndBucketCount(sum, bucketCount);

    Double[] avg = new Double[schema.getNumMetrics()];
    for (int i = 0; i < sum.length; i++) {
      if (bucketCount[i] != 0) {
        avg[i] = sum[i].doubleValue() / (double) bucketCount[i];
      } else {
        avg[i] = valueOfdividedByZero;
      }
    }

    return avg;
  }

  private void calculateMetricSumAndBucketCount(Number[] sum, int[] bucketCount) {
    for (int i = 0; i < schema.getNumMetrics(); i++) {
      sum[i] = 0;
      bucketCount[i] = 0;
    }

    for (Long time : metricsValue.keySet()) {
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        String metricName = schema.getMetricName(i);
        MetricType metricType = schema.getMetricType(i);
        Number metricValue = getOrDefault(time, metricName, null);

        if (metricValue != null) {
          switch (metricType) {
          case INT:
            sum[i] = sum[i].intValue() + metricValue.intValue();
            break;
          case SHORT:
            sum[i] = sum[i].shortValue() + metricValue.shortValue();
            break;
          case LONG:
            sum[i] = sum[i].longValue() + metricValue.longValue();
            break;
          case FLOAT:
            sum[i] = sum[i].floatValue() + metricValue.floatValue();
            break;
          case DOUBLE:
            sum[i] = sum[i].doubleValue() + metricValue.doubleValue();
            break;
          default:
            throw new IllegalStateException();
          }
          ++bucketCount[i];
        }
      }
    }
  }

  public Integer[] getHasValueSums() {
    Integer[] result = new Integer[schema.getNumMetrics()];

    for (int i = 0; i < schema.getNumMetrics(); i++) {
      result[i] = 0;
    }

    for (Long time : hasValue.keySet()) {
      boolean[] booleans = hasValue.get(time);
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        if (booleans[i]) {
          result[i] = result[i] + 1;
        }
      }
    }

    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsValue.keySet(), getSchema());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetricTimeSeries ts = (MetricTimeSeries) o;

    return getTimeWindowSet().equals(ts.getTimeWindowSet())
        && Arrays.equals(getMetricSums(), ts.getMetricSums())
        && Arrays.equals(getHasValueSums(), ts.getHasValueSums());
  }
}
