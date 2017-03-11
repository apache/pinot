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

    Bucket(int[] fromIndex) {
      this.fromIndex = fromIndex;
    }

    public int size() {
      return this.fromIndex.length;
    }
  }

  public static class SeriesGrouping {
    final Series keys;
    final Series source;
    final List<Bucket> buckets;

    SeriesGrouping(Series keys, Series source, List<Bucket> buckets) {
      if(keys.size() != buckets.size())
        throw new IllegalArgumentException("key series and bucket count must be equal");
      this.keys = keys;
      this.source = source;
      this.buckets = buckets;
    }

    SeriesGrouping(Series source) {
      this.keys = new LongSeries();
      this.source = source;
      this.buckets = Collections.emptyList();
    }

    public SeriesGrouping applyTo(Series s) {
      return new SeriesGrouping(this.keys, s, this.buckets);
    }

    public int size() {
      return this.keys.size();
    }

    public int sourceSize() {
      return this.source.size();
    }

    public Series keys() {
      return this.keys;
    }

    public Series source() {
      return this.source;
    }

    public boolean isEmpty() {
      return this.keys.isEmpty();
    }

    public DataFrame aggregate(DoubleBatchFunction function) {
      double[] values = new double[this.size()];
      int i = 0;
      for(Bucket b : this.buckets) {
        values[i++] = function.apply(this.source.project(b.fromIndex).toDoubles().values());
      }
      return makeAggregate(this.keys, DataFrame.toSeries(values));
    }

    public DataFrame aggregate(LongBatchFunction function) {
      long[] values = new long[this.size()];
      int i = 0;
      for(Bucket b : this.buckets) {
        values[i++] = function.apply(this.source.project(b.fromIndex).toLongs().values());
      }
      return makeAggregate(this.keys, DataFrame.toSeries(values));
    }

    public DataFrame aggregate(StringBatchFunction function) {
      String[] values = new String[this.size()];
      int i = 0;
      for(Bucket b : this.buckets) {
        values[i++] = function.apply(this.source.project(b.fromIndex).toStrings().values());
      }
      return makeAggregate(this.keys, DataFrame.toSeries(values));
    }

    public DataFrame aggregate(BooleanBatchFunction function) {
      boolean[] values = new boolean[this.size()];
      int i = 0;
      for(Bucket b : this.buckets) {
        values[i++] = function.apply(this.source.project(b.fromIndex).toBooleans().values());
      }
      return makeAggregate(this.keys, DataFrame.toSeries(values));
    }

    static DataFrame makeAggregate(Series keys, Series values) {
      DataFrame df = new DataFrame();
      df.addSeries(COLUMN_KEY, keys);
      df.addSeries(COLUMN_VALUE, values);
      return df;
    }
  }

//  public static final class DoubleBucket extends Bucket {
//    final double key;
//
//    DoubleBucket(double key, int[] fromIndex, Series source) {
//      super(fromIndex, source);
//      this.key = key;
//    }
//
//    public double key() {
//      return key;
//    }
//  }
//
//  public static final class LongBucket extends Bucket {
//    final long key;
//
//    LongBucket(long key, int[] fromIndex, Series source) {
//      super(fromIndex, source);
//      this.key = key;
//    }
//
//    public long key() {
//      return key;
//    }
//  }
//
//  public static final class BooleanBucket extends Bucket {
//    final boolean key;
//
//    BooleanBucket(boolean key, int[] fromIndex, Series source) {
//      super(fromIndex, source);
//      this.key = key;
//    }
//
//    public boolean key() {
//      return key;
//    }
//  }
//
//  public static final class StringBucket extends Bucket {
//    final String key;
//
//    StringBucket(String key, int[] fromIndex, Series source) {
//      super(fromIndex, source);
//      this.key = key;
//    }
//
//    public String key() {
//      return key;
//    }
//  }

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

  public abstract SeriesGrouping groupByValue();

  public SeriesGrouping groupByCount(int bucketSize) {
    if(bucketSize <= 0)
      throw new IllegalArgumentException("bucketSize must be greater than 0");
    if(this.isEmpty())
      return new SeriesGrouping(this);

    bucketSize = Math.min(bucketSize, this.size());

    int numBuckets = (this.size() - 1) / bucketSize + 1;
    long[] keys = new long[numBuckets];
    List<Bucket> buckets = new ArrayList<>();
    for(int i=0; i<numBuckets; i++) {
      int from = i*bucketSize;
      int to = Math.min((i+1)*bucketSize, this.size());
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new Bucket(fromIndex));
      keys[i] = i;
    }
    return new SeriesGrouping(DataFrame.toSeries(keys), this, buckets);
  }

  public SeriesGrouping groupByPartitions(int partitionCount) {
    if(partitionCount <= 0)
      throw new IllegalArgumentException("partitionCount must be greater than 0");
    if(this.isEmpty())
      return new SeriesGrouping(this);

    double perPartition = this.size() /  (double)partitionCount;

    long[] keys = new long[partitionCount];
    List<Bucket> buckets = new ArrayList<>();
    for(int i=0; i<partitionCount; i++) {
      int from = (int)Math.round(i * perPartition);
      int to = (int)Math.round((i+1) * perPartition);
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new Bucket(fromIndex));
      keys[i] = i;
    }
    return new SeriesGrouping(DataFrame.toSeries(keys), this, buckets);
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
}
