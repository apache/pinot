package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.bootstrap.StarTreeBootstrapJob;

public class AggregationKey extends BinaryComparable implements WritableComparable<BinaryComparable> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AggregationKey.class);

  private List<String> dimensionValues;

  public AggregationKey() {

  }

  public AggregationKey(List<String> dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public List<String> getDimensionsValues() {
    return dimensionValues;
  }

  public void setDimensionsValues(List<String> dimensionsValues) {
    this.dimensionValues = dimensionsValues;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long start = System.currentTimeMillis();
    // read the number of dimensions
    int size = in.readInt();
    dimensionValues = new ArrayList<String>(size);
    // for each dimension write the length of each dimension followed by the
    // values
    for (int i = 0; i < size; i++) {
      int length = in.readInt();
      byte[] b = new byte[length];
      in.readFully(b);
      dimensionValues.add(new String(b, "UTF-8"));
    }
    long end = System.currentTimeMillis();
    LOG.info("deser time {}", (end - start));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    long start = System.currentTimeMillis();
    // write the number of dimensions
    out.writeInt(dimensionValues.size());
    // for each dimension write the length of each dimension followed by the
    // values
    for (String dimensionValue : dimensionValues) {
      byte[] bytes = dimensionValue.getBytes("UTF-8");
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    long end = System.currentTimeMillis();
    LOG.info("ser time {}", (end - start));
  }

  public static AggregationKey read(DataInput in) throws IOException {
    AggregationKey key = new AggregationKey();
    key.readFields(in);
    return key;
  }

  public int compareTo(AggregationKey that) {
    // assumes both have the same length
    long start = System.currentTimeMillis();

    int length = Math.min(this.dimensionValues.size(),
        that.dimensionValues.size());
    int ret = 0;
    for (int i = 0; i < length; i++) {
      ret = this.dimensionValues.get(i).compareTo(that.dimensionValues.get(i));
      if (ret != 0) {
        break;
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("compare time {}", (end - start));
    return ret;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dimensionValues);
  }

  @Override
  public boolean equals(Object obj) {
    // assumes both have the same length
    long start = System.currentTimeMillis();
    if (obj instanceof AggregationKey) {
      AggregationKey that = (AggregationKey) obj;
      int length = Math.min(this.dimensionValues.size(),
          that.dimensionValues.size());
      boolean ret = true;
      for (int i = 0; i < length; i++) {
        ret = this.dimensionValues.get(i).equals(that.dimensionValues.get(i));
        if (!ret) {
          break;
        }
      }
      long end = System.currentTimeMillis();
      LOG.info("equals time {}", (end - start));
      return ret;
    }
    return false;
  }
  @Override
  public String toString() {
    return dimensionValues.toString();
  }
  public static void main(String[] args) {
    List<String> dimensionValues = Lists.newArrayList("", "chrome",
        "gmail.com", "android");
    AggregationKey key = new AggregationKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
  }

  @Override
  public byte[] getBytes() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getLength() {
    // TODO Auto-generated method stub
    return 0;
  }
}
