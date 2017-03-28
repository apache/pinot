package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Series container for primitive double.
 */
public final class DoubleSeries extends TypedSeries<DoubleSeries> {
  public static final double NULL_VALUE = Double.NaN;

  public static class DoubleBatchSum implements Series.DoubleFunction {
    @Override
    public double apply(double[] values) {
      // TODO sorted, add low to high for accuracy
      double sum = 0.0d;
      for(double v : values)
        if(!DoubleSeries.isNull(v))
          sum += v;
      return sum;
    }
  }

  public static class DoubleBatchMean implements Series.DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return DoubleSeries.NULL_VALUE;

      // TODO sorted, add low to high for accuracy
      double sum = 0.0d;
      int count = 0;
      for(double v : values) {
        if (!DoubleSeries.isNull(v)) {
          sum += v;
          count++;
        }
      }
      return sum / count;
    }
  }

  public static class DoubleBatchLast implements Series.DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return DoubleSeries.NULL_VALUE;
      return values[values.length - 1];
    }
  }

  public static class Builder extends Series.Builder {
    final List<double[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(double... values) {
      this.arrays.add(values);
      return this;
    }

    public Builder addValues(double value) {
      return this.addValues(new double[] { value });
    }

    public Builder addValues(Collection<Double> values) {
      double[] newValues = new double[values.size()];
      int i = 0;
      for(Double v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addValues(Double... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Double value) {
      return this.addValues(new double[] { valueOf(value) });
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getDoubles().values);
      return this;
    }

    @Override
    public DoubleSeries build() {
      int totalSize = 0;
      for(double[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      double[] values = new double[totalSize];
      for(double[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
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

  public static DoubleSeries empty() {
    return new DoubleSeries();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  final double[] values;

  private DoubleSeries(double... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries getDoubles() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(double value) {
    return value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(double value) {
    if(DoubleSeries.isNull(value))
      return LongSeries.NULL_VALUE;
    return (long) value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(double value) {
    if(DoubleSeries.isNull(value))
      return BooleanSeries.NULL_VALUE;
    return BooleanSeries.valueOf(value != 0.0d);
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(double value) {
    if(DoubleSeries.isNull(value))
      return StringSeries.NULL_VALUE;
    return String.valueOf(value);
  }

  @Override
  public boolean isNull(int index) {
    return isNull(this.values[index]);
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
    return nullSafeDoubleComparator(this.values[indexThis], that.getDouble(indexThat));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static DoubleSeries map(DoubleFunction function, Series... series) {
    if(series.length <= 0)
      return empty();

    DataFrame.assertSameLength(series);

    // Note: code-specialization to help hot-spot vm
    if(series.length == 1)
      return map(function, series[0]);
    if(series.length == 2)
      return map(function, series[0], series[1]);
    if(series.length == 3)
      return map(function, series[0], series[1], series[2]);

    double[] input = new double[series.length];
    double[] output = new double[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static double mapRow(DoubleFunction function, Series[] series, double[] input, int row) {
    for(int j=0; j<series.length; j++) {
      double value = series[j].getDouble(row);
      if(isNull(value))
        return NULL_VALUE;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static DoubleSeries map(DoubleFunction function, Series a) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  private static DoubleSeries map(DoubleFunction function, Series a, Series b) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getDouble(i), b.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  private static DoubleSeries map(DoubleFunction function, Series a, Series b, Series c) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getDouble(i), b.getDouble(i), c.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(DoubleConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    DataFrame.assertSameLength(series);

    double[] input = new double[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(DoubleConditional function, Series[] series, double[] input, int row) {
    for(int j=0; j<series.length; j++) {
      double value = series[j].getDouble(row);
      if(isNull(value))
        return BooleanSeries.NULL_VALUE;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static DoubleSeries aggregate(DoubleFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getDoubles().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(DoubleConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getDoubles().values)).build();
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

  public static double valueOf(Double value) {
    if(value == null)
      return NULL_VALUE;
    return value;
  }

  public static boolean isNull(double value) {
    return Double.isNaN(value);
  }

  private static double[] assertNotEmpty(double[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }
}
