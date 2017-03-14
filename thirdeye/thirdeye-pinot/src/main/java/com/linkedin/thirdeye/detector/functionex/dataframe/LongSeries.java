package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public final class LongSeries extends Series {
  public static final long NULL_VALUE = Long.MIN_VALUE;

  long[] values;

  @FunctionalInterface
  public interface LongFunction {
    long apply(long value);
  }

  @FunctionalInterface
  public interface LongConditional {
    boolean apply(long value);
  }

  public static class LongBatchSum implements Series.LongBatchFunction {
    @Override
    public long apply(long[] values) {
      long sum = 0;
      for(long v : values)
        if(!isNull(v))
          sum += v;
      return sum;
    }
  }

  public static class LongBatchLast implements Series.LongBatchFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length - 1];
    }
  }

  LongSeries(long... values) {
    this.values = values;
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
  public LongSeries copy() {
    return new LongSeries(Arrays.copyOf(this.values, this.values.length));
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

  public LongSeries unique() {
    if(this.values.length <= 0)
      return new LongSeries();

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

    return new LongSeries(Arrays.copyOf(values, uniqueCount));
  }

  public long first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public long last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public LongSeries slice(int from, int to) {
    return new LongSeries(Arrays.copyOfRange(this.values, from, to));
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
  public LongSeries reverse() {
    return (LongSeries)super.reverse();
  }

  public LongSeries map(LongFunction function) {
    assertNotNull();
    return this.mapWithNull(function);
  }

  public LongSeries mapWithNull(LongFunction function) {
    long[] newValues = new long[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new LongSeries(newValues);
  }

  public BooleanSeries map(LongConditional conditional) {
    assertNotNull();
    return this.mapWithNull(conditional);
  }

  public BooleanSeries mapWithNull(LongConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
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
  public LongSeries sorted() {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return new LongSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("LongSeries{");
    for(long l : this.values) {
      builder.append(l);
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
    assertNotEmpty(this.values);
    long m = this.values[0];
    for(long n : this.values) {
      m = Math.min(m, n);
    }
    return m;
  }

  public long max() {
    assertNotEmpty(this.values);
    long m = this.values[0];
    for(long n : this.values) {
      m = Math.max(m, n);
    }
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
      values[i] = this.values[fromIndex[i]];
    }
    return new LongSeries(values);
  }

  public LongSeries fillNull(long value) {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return new LongSeries(values);
  }

  @Override
  public LongSeries shift(int offset) {
    long[] values = new long[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), Math.min(-offset, values.length), NULL_VALUE);
    }
    return new LongSeries(values);
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

//  static List<JoinPair> partialCrossProduct(int l, long[] lval, int[] lref, int r, long[] rval, int[] rref) {
//    List<JoinPair> pairs = new ArrayList<>();
//
//    int lcount = 1;
//    while(l + lcount + 1 < lval.length && lval[lref[l + lcount + 1]] == lval[lref[l + lcount]]) {
//      lcount++;
//    }
//
//    // count similar values on the right
//    int rcount = 1;
//    while(r + rcount + 1 < rval.length && rval[rref[r + rcount + 1]] == rval[rref[r + rcount]]) {
//      rcount++;
//    }
//
//    for(int i=0; i<lcount; i++) {
//      for(int j=0; j<rcount; j++) {
//        pairs.add(new JoinPair(lref[i], rref[j]));
//      }
//    }
//
//    return pairs;
//  }

  @Override
  public SeriesGrouping groupByValue() {
    if(this.isEmpty())
      return new SeriesGrouping(this);

    List<Long> keys = new ArrayList<>();
    List<Bucket> buckets = new ArrayList<>();
    int[] sortedIndex = this.sortedIndex();

    int bucketOffset = 0;
    long lastValue = this.values[sortedIndex[0]];

    for(int i=1; i<sortedIndex.length; i++) {
      long currVal = this.values[sortedIndex[i]];
      if(Long.compare(lastValue, currVal) != 0) {
        int[] fromIndex = Arrays.copyOfRange(sortedIndex, bucketOffset, i);
        keys.add(lastValue);
        buckets.add(new Bucket(fromIndex));
        bucketOffset = i;
        lastValue = currVal;
      }
    }

    int[] fromIndex = Arrays.copyOfRange(sortedIndex, bucketOffset, sortedIndex.length);
    keys.add(lastValue);
    buckets.add(new Bucket(fromIndex));

    return new SeriesGrouping(DataFrame.toSeriesFromLong(keys), this, buckets);
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

  private void assertNotNull() {
    if(hasNull())
      throw new IllegalStateException("Must not contain null values");
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
