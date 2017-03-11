package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public abstract class Series {
  public static final String COLUMN_KEY = "key";
  public static final String COLUMN_VALUE = "value";

  public enum SeriesType {
    DOUBLE,
    LONG,
    STRING,
    BOOLEAN
  }

  @FunctionalInterface
  public interface DoubleBatchFunction {
    double apply(double[] values);
  }

  @FunctionalInterface
  public interface LongBatchFunction {
    long apply(long[] values);
  }

  @FunctionalInterface
  public interface StringBatchFunction {
    String apply(String[] values);
  }

  @FunctionalInterface
  public interface BooleanBatchFunction {
    boolean apply(boolean[] values);
  }

  public static class Bucket {
    final int[] fromIndex;
    final Series source;

    Bucket(int[] fromIndex, Series source) {
      this.fromIndex = fromIndex;
      this.source = source;
    }
    public int size() {
      return this.fromIndex.length;
    }
  }

  public static class SeriesGrouping {
    final Series keys;
    final Bucket[] buckets;

    public SeriesGrouping(Series keys, Bucket[] buckets) {
      this.keys = keys;
      this.buckets = buckets;
    }
    public int size() {
      return this.size();
    }
  }

  public static final class DoubleBucket extends Bucket {
    final double key;

    DoubleBucket(double key, int[] fromIndex, Series source) {
      super(fromIndex, source);
      this.key = key;
    }

    public double key() {
      return key;
    }
  }

  public static final class LongBucket extends Bucket {
    final long key;

    LongBucket(long key, int[] fromIndex, Series source) {
      super(fromIndex, source);
      this.key = key;
    }

    public long key() {
      return key;
    }
  }

  public static final class BooleanBucket extends Bucket {
    final boolean key;

    BooleanBucket(boolean key, int[] fromIndex, Series source) {
      super(fromIndex, source);
      this.key = key;
    }

    public boolean key() {
      return key;
    }
  }

  public static final class StringBucket extends Bucket {
    final String key;

    StringBucket(String key, int[] fromIndex, Series source) {
      super(fromIndex, source);
      this.key = key;
    }

    public String key() {
      return key;
    }
  }

  public static final class JoinPair {
    final int left;
    final int right;

    public JoinPair(int left, int right) {
      this.left = left;
      this.right = right;
    }
  }

  public abstract int size();
  public abstract DoubleSeries toDoubles();
  public abstract LongSeries toLongs();
  public abstract BooleanSeries toBooleans();
  public abstract StringSeries toStrings();
  public abstract SeriesType type();
  public abstract Series slice(int from, int to);
  public abstract Series sorted();
  public abstract Series copy();
  public abstract Series shift(int offset);
  public abstract boolean hasNull();

  abstract Series project(int[] fromIndex);
  abstract List<JoinPair> joinLeft(Series other);

  abstract int[] sortedIndex();
  abstract int[] nullIndex();

  public abstract List<? extends Bucket> groupByValue();

  public List<LongBucket> groupByCount(int bucketSize) {
    if(bucketSize <= 0)
      throw new IllegalArgumentException("bucketSize must be greater than 0");

    bucketSize = Math.min(bucketSize, this.size());

    List<LongBucket> buckets = new ArrayList<>();
    for(int from=0; from<this.size(); from+=bucketSize) {
      int to = Math.min(from+bucketSize, this.size());
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new LongBucket(from / bucketSize, fromIndex, this));
    }
    return buckets;
  }

  public List<LongBucket> groupByPartitions(int partitionCount) {
    if(partitionCount <= 0)
      throw new IllegalArgumentException("partitionCount must be greater than 0");
    if(this.isEmpty())
      return Collections.emptyList();

    double perPartition = this.size() /  (double)partitionCount;

    List<LongBucket> buckets = new ArrayList<>();
    for(int i=0; i<partitionCount; i++) {
      int from = (int)Math.round(i * perPartition);
      int to = (int)Math.round((i+1) * perPartition);
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new LongBucket(i, fromIndex, this));
    }
    return buckets;
  }

  public boolean isEmpty() {
    return this.size() <= 0;
  }
  public Series head(int n) {
    return this.slice(0, Math.min(n, this.size()));
  }
  public Series tail(int n) {
    int len = this.size();
    return this.slice(len - Math.min(n, len), len);
  }
  public Series reverse() {
    int[] fromIndex = new int[this.size()];
    for (int i = 0; i < fromIndex.length; i++) {
      fromIndex[i] = fromIndex.length - i - 1;
    }
    return this.project(fromIndex);
  }
  public Series toType(SeriesType type) {
    return DataFrame.toType(this, type);
  }

  public DataFrame aggregateDouble(List<DoubleBucket> buckets, DoubleBatchFunction aggregator) {
    double[] keys = new double[buckets.size()];
    double[] values = new double[buckets.size()];
    int i = 0;
    for(DoubleBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toDoubles().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateDouble(List<DoubleBucket> buckets, LongBatchFunction aggregator) {
    double[] keys = new double[buckets.size()];
    long[] values = new long[buckets.size()];
    int i = 0;
    for(DoubleBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toLongs().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateDouble(List<DoubleBucket> buckets, StringBatchFunction aggregator) {
    double[] keys = new double[buckets.size()];
    String[] values = new String[buckets.size()];
    int i = 0;
    for(DoubleBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toStrings().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateDouble(List<DoubleBucket> buckets, BooleanBatchFunction aggregator) {
    double[] keys = new double[buckets.size()];
    boolean[] values = new boolean[buckets.size()];
    int i = 0;
    for(DoubleBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toBooleans().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateLong(List<LongBucket> buckets, DoubleBatchFunction aggregator) {
    long[] keys = new long[buckets.size()];
    double[] values = new double[buckets.size()];
    int i = 0;
    for(LongBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toDoubles().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateLong(List<LongBucket> buckets, LongBatchFunction aggregator) {
    long[] keys = new long[buckets.size()];
    long[] values = new long[buckets.size()];
    int i = 0;
    for(LongBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toLongs().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateLong(List<LongBucket> buckets, StringBatchFunction aggregator) {
    long[] keys = new long[buckets.size()];
    String[] values = new String[buckets.size()];
    int i = 0;
    for(LongBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toStrings().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateLong(List<LongBucket> buckets, BooleanBatchFunction aggregator) {
    long[] keys = new long[buckets.size()];
    boolean[] values = new boolean[buckets.size()];
    int i = 0;
    for(LongBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toBooleans().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateString(List<StringBucket> buckets, DoubleBatchFunction aggregator) {
    String[] keys = new String[buckets.size()];
    double[] values = new double[buckets.size()];
    int i = 0;
    for(StringBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toDoubles().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateString(List<StringBucket> buckets, LongBatchFunction aggregator) {
    String[] keys = new String[buckets.size()];
    long[] values = new long[buckets.size()];
    int i = 0;
    for(StringBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toLongs().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateString(List<StringBucket> buckets, StringBatchFunction aggregator) {
    String[] keys = new String[buckets.size()];
    String[] values = new String[buckets.size()];
    int i = 0;
    for(StringBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toStrings().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateString(List<StringBucket> buckets, BooleanBatchFunction aggregator) {
    String[] keys = new String[buckets.size()];
    boolean[] values = new boolean[buckets.size()];
    int i = 0;
    for(StringBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toBooleans().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateBoolean(List<BooleanBucket> buckets, DoubleBatchFunction aggregator) {
    boolean[] keys = new boolean[buckets.size()];
    double[] values = new double[buckets.size()];
    int i = 0;
    for(BooleanBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toDoubles().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateBoolean(List<BooleanBucket> buckets, LongBatchFunction aggregator) {
    boolean[] keys = new boolean[buckets.size()];
    long[] values = new long[buckets.size()];
    int i = 0;
    for(BooleanBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toLongs().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateBoolean(List<BooleanBucket> buckets, StringBatchFunction aggregator) {
    boolean[] keys = new boolean[buckets.size()];
    String[] values = new String[buckets.size()];
    int i = 0;
    for(BooleanBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toStrings().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  public DataFrame aggregateBoolean(List<BooleanBucket> buckets, BooleanBatchFunction aggregator) {
    boolean[] keys = new boolean[buckets.size()];
    boolean[] values = new boolean[buckets.size()];
    int i = 0;
    for(BooleanBucket b : buckets) {
      keys[i] = b.key();
      values[i] = aggregator.apply(b.source.project(b.fromIndex).toBooleans().values());
      i++;
    }
    return makeAggregate(DataFrame.toSeries(keys), DataFrame.toSeries(values));
  }

  static DataFrame makeAggregate(Series keys, Series values) {
    DataFrame df = new DataFrame();
    df.addSeries(COLUMN_KEY, keys);
    df.addSeries(COLUMN_VALUE, values);
    return df;
  }
}
