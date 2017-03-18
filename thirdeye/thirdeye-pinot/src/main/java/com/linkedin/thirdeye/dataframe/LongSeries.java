package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;


/**
 * Series container for primitive long.
 */
public final class LongSeries extends Series {
  public static final long NULL_VALUE = Long.MIN_VALUE;

  // CAUTION: The array is final, but values are inherently modifiable
  final long[] values;

  public static class LongBatchSum implements LongFunction {
    @Override
    public long apply(long[] values) {
      long sum = 0;
      for(long v : values)
        if(!isNull(v))
          sum += v;
      return sum;
    }
  }

  public static class LongBatchLast implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length - 1];
    }
  }

  public static class Builder {
    final List<Long> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder add(long value) {
      this.values.add(value);
      return this;
    }

    public Builder add(Long value) {
      if(value == null) {
        this.values.add(NULL_VALUE);
      } else {
        this.values.add(value);
      }
      return this;
    }

    public Builder add(long... values) {
      return this.add(ArrayUtils.toObject(values));
    }

    public Builder add(Long... values) {
      for(Long v : values)
        this.add(v);
      return this;
    }

    public Builder add(Collection<Long> values) {
      for(Long v : values)
        this.add(v);
      return this;
    }

    public Builder add(LongSeries series) {
      for(long v : series.values)
        this.add(v);
      return this;
    }

    public LongSeries build() {
      long[] values = new long[this.values.size()];
      int i = 0;
      for(Long v : this.values) {
        values[i++] = v;
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

  public static LongSeries buildFrom(Collection<Long> values) {
    return builder().add(values).build();
  }

  public static LongSeries empty() {
    return new LongSeries();
  }

  private LongSeries(long... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries getDoubles() {
    double[] values = new double[this.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(this.values[i])) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = (double) this.values[i];
      }
    }
    return DoubleSeries.buildFrom(values);
  }

  @Override
  public LongSeries getLongs() {
    return this;
  }

  @Override
  public BooleanSeries getBooleans() {
    byte[] values = new byte[this.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(this.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = BooleanSeries.valueOf(this.values[i] != 0L);
      }
    }
    return BooleanSeries.buildFrom(values);
  }

  @Override
  public StringSeries getStrings() {
    String[] values = new String[this.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(this.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(this.values[i]);
      }
    }
    return StringSeries.buildFrom(values);
  }

  @Override
  public LongSeries copy() {
    return buildFrom(Arrays.copyOf(this.values, this.values.length));
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

  @Override
  public LongSeries unique() {
    if(this.values.length <= 0)
      return buildFrom();

    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);

    // first is always unique
    int uniqueCount = 1;

    for(int i=1; i<values.length; i++) {
      if(values[i-1] != values[i]) {
        values[uniqueCount] = values[i];
        uniqueCount++;
      }
    }

    return buildFrom(Arrays.copyOf(values, uniqueCount));
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
  public LongSeries head(int n) {
    return (LongSeries)super.head(n);
  }

  @Override
  public LongSeries tail(int n) {
    return (LongSeries)super.tail(n);
  }

  @Override
  public LongSeries sliceFrom(int from) {
    return (LongSeries)super.sliceFrom(from);
  }

  @Override
  public LongSeries sliceTo(int to) {
    return (LongSeries)super.sliceTo(to);
  }

  @Override
  public LongSeries reverse() {
    return (LongSeries)super.reverse();
  }

  @Override
  public LongSeries sorted() {
    return (LongSeries)super.sorted();
  }

  @Override
  public LongSeries map(LongFunction function) {
    long[] newValues = new long[this.values.length];
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
  public BooleanSeries map(LongConditional conditional) {
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
  public LongSeries aggregate(LongFunction function) {
    return buildFrom(function.apply(this.values));
  }

  @Override
  public LongSeries append(Series series) {
    long[] values = new long[this.size() + series.size()];
    System.arraycopy(this.values, 0, values, 0, this.size());
    System.arraycopy(series.getLongs().values, 0, values, this.size(), series.size());
    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    List<LongSortTuple> tuples = new ArrayList<>();
    for(int i=0; i<this.values.length; i++) {
      tuples.add(new LongSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<LongSortTuple>() {
      @Override
      public int compare(LongSortTuple a, LongSortTuple b) {
        return Long.compare(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
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
   * <b>value</b>.
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
  public boolean hasNull() {
    for(long v : this.values)
      if(isNull(v))
        return true;
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

    LongSeries that = (LongSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return Long.compare(this.values[indexThis], ((LongSeries)that).values[indexThat]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(long value) {
    return value == NULL_VALUE;
  }

  private static long[] assertNotEmpty(long[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static final class LongSortTuple {
    final long value;
    final int index;

    LongSortTuple(long value, int index) {
      this.value = value;
      this.index = index;
    }
  }

  static class Range {
    final long lower;
    final long upper; // exclusive

    Range(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }
}
