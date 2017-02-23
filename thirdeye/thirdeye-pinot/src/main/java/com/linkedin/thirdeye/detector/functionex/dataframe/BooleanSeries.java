package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.NumberUtils;


public final class BooleanSeries extends Series {
  boolean[] values;

  @FunctionalInterface
  public interface BooleanBatchFunction {
    boolean apply(boolean[] values);
  }

  public static class BooleanBatchAnd implements BooleanBatchFunction {
    @Override
    public boolean apply(boolean[] values) {
      for(boolean b : values) {
        if(!b)
          return false;
      }
      return true;
    }
  }

  public static class BooleanBatchOr implements BooleanBatchFunction {
    @Override
    public boolean apply(boolean[] values) {
      for(boolean b : values) {
        if(b)
          return true;
      }
      return false;
    }
  }

  public static class BooleanBatchLast implements BooleanBatchFunction {
    @Override
    public boolean apply(boolean[] values) {
      return values[values.length-1];
    }
  }

  BooleanSeries(boolean[] values) {
    this.values = Arrays.copyOf(values, values.length);
  }

  BooleanSeries(double[] values) {
    this.values = new boolean[values.length];
    for(int i=0; i<values.length; i++) {
      this.values[i] = values[i] != 0.0d;
    }
  }

  BooleanSeries(long[] values) {
    this.values = new boolean[values.length];
    for(int i=0; i<values.length; i++) {
      this.values[i] = values[i] != 0l;
    }
  }

  BooleanSeries(String[] values) {
    this.values = new boolean[values.length];
    for(int i=0; i<values.length; i++) {
      if(NumberUtils.isNumber(values[i]))
        this.values[i] = Double.parseDouble(values[i]) != 0.0d;
      else
        this.values[i] = Boolean.parseBoolean(values[i]);
    }
  }

  @Override
  public BooleanSeries copy() {
    return new BooleanSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries toDoubles() {
    return new DoubleSeries(this.values);
  }

  @Override
  public LongSeries toLongs() {
    return new LongSeries(this.values);
  }

  @Override
  public BooleanSeries toBooleans() {
    return this;
  }

  @Override
  public StringSeries toStrings() {
    return new StringSeries(this.values);
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.BOOLEAN;
  }

  public boolean[] values() {
    return this.values;
  }

  public boolean first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public boolean last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public BooleanSeries slice(int from, int to) {
    return new BooleanSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public BooleanSeries head(int n) {
    return (BooleanSeries) super.head(n);
  }

  @Override
  public BooleanSeries tail(int n) {
    return (BooleanSeries) super.tail(n);
  }

  @Override
  public BooleanSeries reverse() {
    return (BooleanSeries)super.reverse();
  }

  public boolean allTrue() {
    boolean result = true;
    for(boolean b : this.values) {
      result &= b;
    }
    return result;
  }

  public boolean hasTrue() {
    boolean result = false;
    for(boolean b : this.values) {
      result |= b;
    }
    return result;
  }

  public boolean allFalse() {
    return !hasTrue();
  }

  public boolean hasFalse() {
    return !allTrue();
  }

  public BooleanSeries not() {
    boolean[] bvalues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      bvalues[i] = !this.values[i];
    }
    return new BooleanSeries(bvalues);
  }

  @Override
  BooleanSeries reorder(int[] toIndex) {
    int len = this.values.length;
    if(toIndex.length != len)
      throw new IllegalArgumentException("toIndex size does not equal series size");

    boolean[] values = new boolean[len];
    for(int i=0; i<len; i++) {
      values[toIndex[i]] = this.values[i];
    }
    return new BooleanSeries(values);
  }

  @Override
  public BooleanSeries sort() {
    boolean[] values = new boolean[this.values.length];
    int count_false = 0;

    // count true
    for(int i=0; i<this.values.length; i++) {
      if(!this.values[i])
        count_false++;
    }

    // first false, then true
    Arrays.fill(values, 0, count_false, false);
    Arrays.fill(values, count_false, values.length, true);

    return new BooleanSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BooleanSeries{");
    for(boolean b : this.values) {
      builder.append(b);
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  // TODO bucketsBy...

  public BooleanSeries groupBy(List<Bucket> buckets, BooleanBatchFunction grouper) {
    return this.groupBy(buckets, false, grouper);
  }

  public BooleanSeries groupBy(List<Bucket> buckets, boolean nullValue, BooleanBatchFunction grouper) {
    boolean[] values = new boolean[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      Bucket b = buckets.get(i);

      // no elements in group
      if(b.fromIndex.length <= 0) {
        values[i] = nullValue;
        continue;
      }

      // group
      boolean[] gvalues = new boolean[b.fromIndex.length];
      for(int j=0; j<gvalues.length; j++) {
        gvalues[j] = this.values[b.fromIndex[j]];
      }
      values[i] = grouper.apply(gvalues);
    }
    return new BooleanSeries(values);
  }

  @Override
  public BooleanSeries filter(int[] fromIndex) {
    boolean[] values = new boolean[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
    }
    return new BooleanSeries(values);
  }

  private static boolean[] assertNotEmpty(boolean[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  @Override
  int[] sortedIndex() {
    int[] toIndex = new int[this.values.length];
    int j=0;

    // false first
    for(int i=0; i<this.values.length; i++) {
      if(!this.values[i])
        toIndex[i] = j++;
    }

    // then true
    for(int i=0; i<this.values.length; i++) {
      if(this.values[i])
        toIndex[i] = j++;
    }

    return toIndex;
  }

}
