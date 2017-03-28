package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * Series container for primitive long.
 */
public final class LongSeries extends TypedSeries<LongSeries> {
  public static final long NULL_VALUE = Long.MIN_VALUE;

  public static class LongBatchSum implements Series.LongFunction {
    @Override
    public long apply(long[] values) {
      long sum = 0;
      for(long v : values)
        if(!LongSeries.isNull(v))
          sum += v;
      return sum;
    }
  }

  public static class LongBatchLast implements Series.LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return LongSeries.NULL_VALUE;
      return values[values.length - 1];
    }
  }

  public static class Builder extends Series.Builder {
    final List<long[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(long... values) {
      this.arrays.add(values);
      return this;
    }

    public Builder addValues(long value) {
      return this.addValues(new long[] { value });
    }

    public Builder addValues(Collection<Long> values) {
      long[] newValues = new long[values.size()];
      int i = 0;
      for(Long v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addValues(Long... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Long value) {
      return this.addValues(new long[] { valueOf(value) });
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getLongs().values);
      return this;
    }

    @Override
    public LongSeries build() {
      int totalSize = 0;
      for(long[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      long[] values = new long[totalSize];
      for(long[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return new LongSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static LongSeries buildFrom(long... values) {
    return new LongSeries(values);
  }

  public static LongSeries empty() {
    return new LongSeries();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  final long[] values;

  private LongSeries(long... values) {
    this.values = values;
  }

  @Override
  public LongSeries getLongs() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(long value) {
    if(LongSeries.isNull(value))
      return DoubleSeries.NULL_VALUE;
    return (double) value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(long value) {
    return value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(long value) {
    if(LongSeries.isNull(value))
      return BooleanSeries.NULL_VALUE;
    return BooleanSeries.valueOf(value != 0L);
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(long value) {
    if(LongSeries.isNull(value))
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
    return SeriesType.LONG;
  }

  public long[] values() {
    return this.values;
  }

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public long first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
  public long last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public LongSeries slice(int from, int to) {
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("LongSeries{");
    for(long l : this.values) {
      if(isNull(l)) {
        builder.append("null");
      } else {
        builder.append(l);
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  public SeriesGrouping groupByInterval(long interval) {
    if(interval <= 0)
      throw new IllegalArgumentException("interval must be greater than 0");
    if(this.size() <= 0)
      return new SeriesGrouping(this);

    long start = this.min() / interval; // align with interval
    long stop = this.max() / interval + 1;

    List<Range> ranges = new ArrayList<>();
    for(long i=start; i<stop; i++) {
      ranges.add(new Range(i * interval, (i+1) * interval));
    }

    // turn ranges into buckets from original series
    // TODO use nlogm solution to find matching range, e.g. ordered tree
    long[] keys = new long[ranges.size()];
    List<Bucket> buckets = new ArrayList<>();

    int i = 0;
    for(Range r : ranges) {
      ArrayList<Integer> ind = new ArrayList<>();
      for(int j=0; j<this.size(); j++) {
        if(this.values[j] >= r.lower && this.values[j] < r.upper) {
          ind.add(j);
        }
      }

      int[] fromIndex = new int[ind.size()];
      for(int j=0; j<ind.size(); j++) {
        fromIndex[j] = ind.get(j);
      }

      buckets.add(new Bucket(fromIndex));
      keys[i++] = r.lower;
    }

    return new SeriesGrouping(DataFrame.toSeries(keys), this, buckets);
  }

  public long min() {
    long m = NULL_VALUE;
    for(long n : this.values) {
      if(!isNull(n) && (isNull(m) || n < m))
        m = n;
    }
    if(isNull(m))
      throw new IllegalStateException("No valid minimum value");
    return m;
  }

  public long max() {
    long m = NULL_VALUE;
    for(long n : this.values) {
      if(!isNull(n) && (isNull(m) || n > m))
        m = n;
    }
    if(isNull(m))
      throw new IllegalStateException("No valid maximum value");
    return m;
  }

  public double mean() {
    assertNotEmpty(this.values);
    long sum = 0;
    int count = 0;
    for(long v : this.values) {
      if(!isNull(v)) {
        sum += v;
        count++;
      }
    }
    return sum / (double) count;
  }

  public long sum() {
    return new LongBatchSum().apply(this.values);
  }

  @Override
  LongSeries project(int[] fromIndex) {
    long[] values = new long[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return buildFrom(values);
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public LongSeries fillNull(long value) {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
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

    LongSeries that = (LongSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return Long.compare(this.values[indexThis], that.getLong(indexThat));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static LongSeries map(LongFunction function, Series... series) {
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

    long[] input = new long[series.length];
    long[] output = new long[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static long mapRow(LongFunction function, Series[] series, long[] input, int row) {
    for(int j=0; j<series.length; j++) {
      long value = series[j].getLong(row);
      if(isNull(value))
        return NULL_VALUE;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static LongSeries map(LongFunction function, Series a) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getLong(i));
      }
    }
    return buildFrom(output);
  }

  private static LongSeries map(LongFunction function, Series a, Series b) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getLong(i), b.getLong(i));
      }
    }
    return buildFrom(output);
  }

  private static LongSeries map(LongFunction function, Series a, Series b, Series c) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getLong(i), b.getLong(i), c.getLong(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static BooleanSeries map(LongConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    DataFrame.assertSameLength(series);

    long[] input = new long[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(LongConditional function, Series[] series, long[] input, int row) {
    for(int j=0; j<series.length; j++) {
      long value = series[j].getLong(row);
      if(isNull(value))
        return BooleanSeries.NULL_VALUE;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static LongSeries aggregate(LongFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getLongs().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(LongConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getLongs().values)).build();
  }

  public static long valueOf(Long value) {
    if(value == null)
      return NULL_VALUE;
    return value;
  }

  public static boolean isNull(long value) {
    return value == NULL_VALUE;
  }

  private static long[] assertNotEmpty(long[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static class Range {
    final long lower;
    final long upper; // exclusive

    Range(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  @Override
  public LongSeries shift(int offset) {
    long[] values = new long[this.values.length];
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
  public LongSeries sorted() {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    List<LongSortTuple> tuples = new ArrayList<>();
    for (int i = 0; i < this.values.length; i++) {
      tuples.add(new LongSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<LongSortTuple>() {
      @Override
      public int compare(LongSortTuple a, LongSortTuple b) {
        return Long.compare(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for (int i = 0; i < tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  static final class LongSortTuple {
    final long value;
    final int index;

    LongSortTuple(long value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
