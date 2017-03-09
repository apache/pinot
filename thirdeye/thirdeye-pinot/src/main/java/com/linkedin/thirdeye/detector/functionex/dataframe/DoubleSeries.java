package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public final class DoubleSeries extends Series {
  public static final double NULL_VALUE = Double.NaN;

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
      // TODO sorted, add low to high for accuracy
      double sum = 0.0d;
      for(double v : values)
        if(!isNull(v))
          sum += v;
      return sum;
    }
  }

  public static class DoubleBatchMean implements DoubleBatchFunction {
    @Override
    public double apply(double[] values) {
      assertNotEmpty(values);
      // TODO sorted, add low to high for accuracy
      double sum = 0.0d;
      int count = 0;
      for(double v : values) {
        if (!isNull(v)) {
          sum += v;
          count++;
        }
      }
      return sum / count;
    }
  }

  public static class DoubleBatchLast implements DoubleBatchFunction {
    @Override
    public double apply(double[] values) {
      return values[values.length - 1];
    }
  }

  DoubleSeries(double... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries copy() {
    return new DoubleSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries toDoubles() {
    return DataFrame.toDoubles(this);
  }

  @Override
  public LongSeries toLongs() {
    return DataFrame.toLongs(this);
  }

  @Override
  public BooleanSeries toBooleans() {
    return DataFrame.toBooleans(this);
  }

  @Override
  public StringSeries toStrings() {
    return DataFrame.toStrings(this);
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

  public DoubleSeries unique() {
    if(this.values.length <= 0)
      return new DoubleSeries();

    double[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);

    // first is always unique
    int uniqueCount = 1;

    for(int i=1; i<values.length; i++) {
      if(values[i-1] != values[i]) {
        values[uniqueCount] = values[i];
        uniqueCount++;
      }
    }

    return new DoubleSeries(Arrays.copyOf(values, uniqueCount));
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
    assertNotNull();
    return this.mapWithNull(function);
  }

  public DoubleSeries mapWithNull(DoubleFunction function) {
    double[] newValues = new double[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new DoubleSeries(newValues);
  }

  public BooleanSeries map(DoubleConditional conditional) {
    assertNotNull();
    return this.mapWithNull(conditional);
  }

  public BooleanSeries mapWithNull(DoubleConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  public DoubleSeries applyMovingWindow(int size, int minSize, DoubleBatchFunction function) {
    double[] values = new double[this.values.length];

    // fill minSize - 1 with null values
    Arrays.fill(values, 0, Math.min(values.length, Math.max(0, minSize)), NULL_VALUE);

    for(int to=Math.max(1, minSize); to<=values.length; to++) {
      int from = Math.max(0, to - size);
      double[] input = Arrays.copyOfRange(this.values, from, to);
      values[to-1] = function.apply(input);
    }

    return new DoubleSeries(values);
  }

  @Override
  public DoubleSeries sorted() {
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
    return this.groupBy(buckets, NULL_VALUE, grouper);
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

  public DoubleSeries fillNull(double value) {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return new DoubleSeries(values);
  }

  @Override
  public DoubleSeries shift(int offset) {
    double[] values = new double[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), Math.min(-offset, values.length), NULL_VALUE);
    }
    return new DoubleSeries(values);
  }

  @Override
  DoubleSeries project(int[] fromIndex) {
    double[] values = new double[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
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
        return Double.compare(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  @Override
  public boolean hasNull() {
    for(double v : this.values) {
      if(isNull(v))
        return true;
    }
    return false;
  }

  @Override
  int[] nullIndex() {
    int[] nulls = new int[this.values.length];
    int nullCount = 0;

    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i])) {
        nulls[nullCount] = i;
        nullCount++;
      }
    }

    return Arrays.copyOf(nulls, nullCount);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DoubleSeries that = (DoubleSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(double value) {
    return Double.isNaN(value);
  }

  private void assertNotNull() {
    if(hasNull())
      throw new IllegalStateException("Must not contain null values");
  }

  private static double[] assertNotEmpty(double[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
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
