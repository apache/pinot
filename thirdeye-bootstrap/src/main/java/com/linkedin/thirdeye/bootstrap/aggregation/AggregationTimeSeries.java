package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

/**
 * 
 * @author kgopalak
 * 
 */
public class AggregationTimeSeries implements Writable {
  private static final Logger LOG = LoggerFactory
      .getLogger(AggregationTimeSeries.class);
  String[] dimensions;
  Map<Long, Number[]> timeseries;
  private List<String> types;
  private static Kryo kryo = new Kryo();
  private static Output output;

  static {
    kryo.register(AggregationTimeSeries.class);
    output = new Output(new ByteOutputStream());
  }

  public AggregationTimeSeries() {

  }

  /**
   * 
   * @param types
   */
  public AggregationTimeSeries(List<String> types) {
    this.types = types;
    timeseries = new HashMap<Long, Number[]>();
  }

  public void set(long timeWindow, int index, Number value) {
    if (!timeseries.containsKey(timeWindow)) {
      timeseries.put(timeWindow, new Number[types.size()]);
    }
    Number[] numbers = timeseries.get(timeWindow);
    numbers[index] = value;
  }

  public void increment(long timeWindow, int index, Number value) {
    if (!timeseries.containsKey(timeWindow)) {
      timeseries.put(timeWindow, new Number[types.size()]);
    }
    Number[] numbers = timeseries.get(timeWindow);
    if ("int".equals(types.get(index))) {
      numbers[index] = numbers[index].intValue() + value.intValue();
      return;
    }
    if ("long".equals(types.get(index))) {
      numbers[index] = numbers[index].longValue() + value.longValue();
      return;
    }
    if ("float".equals(types.get(index))) {
      numbers[index] = numbers[index].floatValue() + value.floatValue();
      return;
    }
    if ("double".equals(types.get(index))) {
      numbers[index] = numbers[index].doubleValue() + value.doubleValue();
      return;
    }

  }

  public void decrement(long timeWindow, int index, Number value) {
    if (!timeseries.containsKey(timeWindow)) {
      timeseries.put(timeWindow, new Number[types.size()]);
    }
    Number[] numbers = timeseries.get(timeWindow);
    if ("int".equals(types.get(index))) {
      numbers[index] = numbers[index].intValue() - value.intValue();
      return;
    }
    if ("long".equals(types.get(index))) {
      numbers[index] = numbers[index].longValue() - value.longValue();
      return;
    }
    if ("float".equals(types.get(index))) {
      numbers[index] = numbers[index].floatValue() - value.floatValue();
      return;
    }
    if ("double".equals(types.get(index))) {
      numbers[index] = numbers[index].doubleValue() - value.doubleValue();
      return;
    }
  }

  public void aggregate(AggregationTimeSeries series) {
    for (long timeWindow : series.timeseries.keySet()) {
      Number[] value = series.timeseries.get(timeWindow);
      if (timeseries.containsKey(timeWindow)) {
        timeseries.put(timeWindow, value);
      } else {
        for (int i = 0; i < value.length; i++) {
          this.increment(timeWindow, i, value[i]);
        }
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long start = System.currentTimeMillis();
    int length = in.readInt();
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    Input input = new Input(buffer);
    AggregationTimeSeries obj = kryo.readObject(input,
        AggregationTimeSeries.class);
    this.timeseries = obj.timeseries;
    this.types = obj.types;
    long end = System.currentTimeMillis();
    LOG.info("deser time series  {}", (end - start));

  }

  @Override
  public void write(DataOutput out) throws IOException {
    long start = System.currentTimeMillis();

    output.clear();
    kryo.writeObject(output, this);
    byte[] buffer = output.getBuffer();
    out.writeInt(buffer.length);
    out.write(buffer);
    long end = System.currentTimeMillis();
    LOG.info("ser time series  {}", (end - start));
  }

}
