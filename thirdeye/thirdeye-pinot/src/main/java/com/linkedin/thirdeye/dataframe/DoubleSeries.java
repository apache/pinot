package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;


/**
 * Series container for primitive double.
 */
public final class DoubleSeries extends Series {
  public static final double NULL_VALUE = Double.NaN;

  // CAUTION: The array is final, but values are inherently modifiable
  final double[] values;

  public static class DoubleBatchSum implements DoubleFunction {
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

  public static class DoubleBatchMean implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL_VALUE;

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

  public static class DoubleBatchLast implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length - 1];
    }
  }

  public static class Builder {
    final List<Double> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder add(double value) {
      this.values.add(value);
      return this;
    }

    public Builder add(Double value) {
      if(value == null) {
        this.values.add(NULL_VALUE);
      } else {
        this.values.add(value);
      }
      return this;
    }

    public Builder add(double... values) {
      return this.add(ArrayUtils.toObject(values));
    }

    public Builder add(Double... values) {
      for(Double v : values)
        this.add(v);
      return this;
    }

    public Builder add(Collection<Double> values) {
      for(Double v : values)
        this.add(v);
      return this;
    }

    public Builder add(DoubleSeries series) {
      for(double v : series.values)
        this.add(v);
      return this;
    }

    public DoubleSeries build() {
      double[] values = new double[this.values.size()];
      int i = 0;
      for(Double v : this.values) {
        values[i++] = v;
      }
      return new DoubleSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DoubleSeries buildFrom(double... values) {
    return new DoubleSeries(values);
  }

  public static DoubleSeries buildFrom(Collection<Double> values) {
    return builder().add(values).build();
  }

  public static DoubleSeries empty() {
    return new DoubleSeries();
  }

  private DoubleSeries(double... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries copy() {
    return buildFrom(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries getDoubles() {
    return this;
  }

  @Override
  public LongSeries getLongs() {
    long[] values = new long[this.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(this.values[i])) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        values[i] = (long) this.values[i];
      }
    }
    return LongSeries.buildFrom(values);
  }

  @Override
  public BooleanSeries getBooleans() {
    byte[] values = new byte[this.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(this.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = BooleanSeries.valueOf(this.values[i] != 0.0d);
      }
    }
    return BooleanSeries.buildFrom(values);
  }

  @Override
  public StringSeries getStrings() {
    String[] values = new String[this.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(this.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(this.values[i]);
      }
    }
    return StringSeries.buildFrom(values);
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

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public double first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
  public double last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public DoubleSeries slice(int from, int to) {
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
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
  public DoubleSeries sliceFrom(int from) {
    return (DoubleSeries)super.sliceFrom(from);
  }

  @Override
  public DoubleSeries sliceTo(int to) {
    return (DoubleSeries)super.sliceTo(to);
  }

  @Override
  public DoubleSeries reverse() {
    return (DoubleSeries)super.reverse();
  }

  @Override
  public DoubleSeries sorted() {
    return (DoubleSeries)super.sorted();
  }

  @Override
  public DoubleSeries unique() {
    return (DoubleSeries)super.unique();
  }

  @Override
  public DoubleSeries map(DoubleFunction function) {
    double[] newValues = new double[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i])) {
        newValues[i] = NULL_VALUE;
      } else {
        newValues[i] = function.apply(this.values[i]);
      }
    }
    return buildFrom(newValues);
  }

  @Override
  public BooleanSeries map(DoubleConditional conditional) {
    byte[] newValues = new byte[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i])) {
        newValues[i] = BooleanSeries.NULL_VALUE;
      } else {
        newValues[i] = BooleanSeries.valueOf(conditional.apply(this.values[i]));
      }
    }
    return BooleanSeries.buildFrom(newValues);
  }

  @Override
  public DoubleSeries aggregate(DoubleFunction function) {
    return buildFrom(function.apply(this.values));
  }

  @Override
  public DoubleSeries append(Series series) {
    double[] values = new double[this.size() + series.size()];
    System.arraycopy(this.values, 0, values, 0, this.size());
    System.arraycopy(series.getDoubles().values, 0, values, this.size(), series.size());
    return buildFrom(values);
  }

  public DoubleSeries applyMovingWindow(int size, int minSize, DoubleFunction function) {
    double[] values = new double[this.values.length];

    // fill minSize - 1 with null values
    Arrays.fill(values, 0, Math.min(values.length, Math.max(0, minSize)), NULL_VALUE);

    for(int to=Math.max(1, minSize); to<=values.length; to++) {
      int from = Math.max(0, to - size);
      double[] input = Arrays.copyOfRange(this.values, from, to);
      values[to-1] = function.apply(input);
    }

    return buildFrom(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DoubleSeries{");
    for(double d : this.values) {
      if(isNull(d)) {
        builder.append("null");
      } else {
        builder.append(d);
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  public double min() {
    double m = NULL_VALUE;
    for(double n : this.values) {
      if(!isNull(n) && (isNull(m) || n < m))
        m = n;
    }
    if(isNull(m))
      throw new IllegalStateException("No valid minimum value");
    return m;
  }

  public double max() {
    double m = NULL_VALUE;
    for(double n : this.values) {
      if(!isNull(n) && (isNull(m) || n > m))
        m = n;
    }
    if(isNull(m))
      throw new IllegalStateException("No valid maximum value");
    return m;
  }

  public double mean() {
    assertNotEmpty(this.values);
    return new DoubleBatchMean().apply(this.values);
  }

  public double sum() {
    return new DoubleBatchSum().apply(this.values);
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public DoubleSeries fillNull(double value) {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  public DoubleSeries shift(int offset) {
    double[] values = new double[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL_VALUE);
    }
    return buildFrom(values);
  }

  @Override
  DoubleSeries project(int[] fromIndex) {
    double[] values = new double[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return buildFrom(values);
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
        return nullSafeDoubleComparator(a.value, b.value);
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
  int compare(Series that, int indexThis, int indexThat) {
    return nullSafeDoubleComparator(this.values[indexThis], ((DoubleSeries)that).values[indexThat]);
  }

  private static int nullSafeDoubleComparator(double a, double b) {
    if(isNull(a) && isNull(b))
      return 0;
    if(isNull(a))
      return -1;
    if(isNull(b))
      return 1;
    return Double.compare(a, b);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(double value) {
    return Double.isNaN(value);
  }

  private static double[] assertNotEmpty(double[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static final class DoubleSortTuple {
    final double value;
    final int index;

    DoubleSortTuple(double value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
