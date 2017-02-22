package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public final class DoubleSeries extends Series {
  double[] values;

  @FunctionalInterface
  public interface DoubleFunction {
    double apply(double value);
  }

  @FunctionalInterface
  public interface DoubleConditional {
    boolean apply(double value);
  }

  @FunctionalInterface
  public interface DoubleBatchFunction {
    double apply(double[] values);
  }

  public static class DoubleBatchSum implements DoubleBatchFunction {
    @Override
    public double apply(double[] values) {
      // TODO sort, add low to high for accuracy
      double sum = 0;
      for(double l : values)
        sum += l;
      return sum;
    }
  }

  public static class DoubleBatchMean implements DoubleBatchFunction {
    @Override
    public double apply(double[] values) {
      assertNotEmpty(values);
      // TODO sort, add low to high for accuracy
      double sum = 0;
      for(double l : values)
        sum += l;
      return sum / values.length;
    }
  }

  public static class DoubleBatchLast implements DoubleBatchFunction {
    @Override
    public double apply(double[] values) {
      return values[values.length - 1];
    }
  }

  DoubleSeries(double[] values) {
    this.values = Arrays.copyOf(values, values.length);
  }

  DoubleSeries(long[] values) {
    this.values = new double[values.length];
    for(int i=0; i<values.length; i++) {
      this.values[i] = (double) values[i];
    }
  }

  DoubleSeries(boolean[] values) {
    this.values = new double[values.length];
    for(int i=0; i<values.length; i++) {
      this.values[i] = values[i] ? 1.0d : 0.0d;
    }
  }

  DoubleSeries(String[] values) {
    this.values = new double[values.length];
    for(int i=0; i<values.length; i++) {
      this.values[i] = Double.parseDouble(values[i]);
    }
  }

  @Override
  public DoubleSeries copy() {
    return new DoubleSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries toDoubles() {
    return this;
  }

  @Override
  public LongSeries toLongs() {
    return new LongSeries(this.values);
  }

  @Override
  public BooleanSeries toBooleans() {
    return new BooleanSeries(this.values);
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
    return SeriesType.DOUBLE;
  }

  public double[] values() {
    return this.values;
  }

  public double first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public double last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public DoubleSeries slice(int from, int to) {
    return new DoubleSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public DoubleSeries head(int n) {
    return (DoubleSeries)super.head(n);
  }

  @Override
  public DoubleSeries tail(int n) {
    return (DoubleSeries)super.tail(n);
  }

  @Override
  public DoubleSeries reverse() {
    return (DoubleSeries)super.reverse();
  }

  public DoubleSeries map(DoubleFunction function) {
    double[] newValues = new double[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new DoubleSeries(newValues);
  }

  public BooleanSeries map(DoubleConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  @Override
  public DoubleSeries reorder(int[] toIndex) {
    int len = this.values.length;
    if(toIndex.length != len)
      throw new IllegalArgumentException("toIndex size does not equal series size");

    double[] values = new double[len];
    for(int i=0; i<len; i++) {
      values[toIndex[i]] = this.values[i];
    }
    return new DoubleSeries(values);
  }

  @Override
  int[] sortedIndex() {
    List<DoubleSortTuple> tuples = new ArrayList<>();
    for(int i=0; i<this.values.length; i++) {
      tuples.add(new DoubleSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<DoubleSortTuple>() {
      @Override
      public int compare(DoubleSortTuple a, DoubleSortTuple b) {
        return a.value == b.value ? 0 : a.value <= b.value ? -1 : 1;
      }
    });

    int[] toIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      toIndex[tuples.get(i).index] = i;
    }
    return toIndex;
  }

  @Override
  public DoubleSeries sort() {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return new DoubleSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DoubleSeries{");
    for(double d : this.values) {
      builder.append(d);
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  // TODO bucketsBy...

  public DoubleSeries groupBy(List<Bucket> buckets, DoubleBatchFunction grouper) {
    return this.groupBy(buckets, Double.MIN_VALUE, grouper);
  }

  public DoubleSeries groupBy(List<Bucket> buckets, double nullValue, DoubleBatchFunction grouper) {
    double[] values = new double[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      Bucket b = buckets.get(i);

      // no elements in group
      if(b.fromIndex.length <= 0) {
        values[i] = nullValue;
        continue;
      }

      // group
      double[] gvalues = new double[b.fromIndex.length];
      for(int j=0; j<gvalues.length; j++) {
        gvalues[j] = this.values[b.fromIndex[j]];
      }
      values[i] = grouper.apply(gvalues);
    }
    return new DoubleSeries(values);
  }

  public double min() {
    assertNotEmpty(this.values);
    double m = this.values[0];
    for(double n : this.values) {
      m = Math.min(m, n);
    }
    return m;
  }

  public double max() {
    assertNotEmpty(this.values);
    double m = this.values[0];
    for(double n : this.values) {
      m = Math.max(m, n);
    }
    return m;
  }

  public double mean() {
    return new DoubleBatchMean().apply(this.values);
  }

  public double sum() {
    return new DoubleBatchSum().apply(this.values);
  }

  private static double[] assertNotEmpty(double[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  @Override
  public DoubleSeries filter(int[] fromIndex) {
    double[] values = new double[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
    }
    return new DoubleSeries(values);
  }

  static final class DoubleSortTuple {
    final double value;
    final int index;

    public DoubleSortTuple(double value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
