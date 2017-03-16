package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
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

  enum JoinType {
    INNER,
    OUTER,
    LEFT,
    RIGHT
  }

  @FunctionalInterface
  public interface DoubleConditional {
    boolean apply(double value);
  }

  @FunctionalInterface
  public interface LongConditional {
    boolean apply(long value);
  }

  @FunctionalInterface
  public interface StringConditional {
    boolean apply(String value);
  }

  @FunctionalInterface
  public interface DoubleFunction {
    double apply(double... values);
  }

  @FunctionalInterface
  public interface LongFunction {
    long apply(long... values);
  }

  @FunctionalInterface
  public interface StringFunction {
    String apply(String... values);
  }

  @FunctionalInterface
  public interface BooleanFunction {
    boolean apply(boolean... values);
  }

  public static final class Bucket {
    final int[] fromIndex;

    Bucket(int[] fromIndex) {
      this.fromIndex = fromIndex;
    }

    public int size() {
      return this.fromIndex.length;
    }
  }

  public static final class SeriesGrouping {
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

    public DataFrame aggregate(DoubleFunction function) {
      DoubleSeries s = new DoubleSeries();
      for(Bucket b : this.buckets)
        s = s.append(this.source.project(b.fromIndex).aggregate(function));
      return makeAggregate(this.keys, s);
    }

    public DataFrame aggregate(LongFunction function) {
      LongSeries s = new LongSeries();
      for(Bucket b : this.buckets)
        s = s.append(this.source.project(b.fromIndex).aggregate(function));
      return makeAggregate(this.keys, s);
    }

    public DataFrame aggregate(StringFunction function) {
      StringSeries s = new StringSeries();
      for(Bucket b : this.buckets)
        s = s.append(this.source.project(b.fromIndex).aggregate(function));
      return makeAggregate(this.keys, s);
    }

    public DataFrame aggregate(BooleanFunction function) {
      BooleanSeries s = new BooleanSeries();
      for(Bucket b : this.buckets)
        s = s.append(this.source.project(b.fromIndex).aggregate(function));
      return makeAggregate(this.keys, s);
    }

    static DataFrame makeAggregate(Series keys, Series values) {
      DataFrame df = new DataFrame();
      df.addSeries(COLUMN_KEY, keys);
      df.addSeries(COLUMN_VALUE, values);
      return df;
    }
  }

  static final class JoinPair {
    final int left;
    final int right;

    public JoinPair(int left, int right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      JoinPair joinPair = (JoinPair) o;

      return (left == joinPair.left) && (right == joinPair.right);
    }

    @Override
    public int hashCode() {
      int result = left;
      result = 31 * result + right;
      return result;
    }
  }

  public abstract int size();
  public abstract DoubleSeries getDoubles();
  public abstract LongSeries getLongs();
  public abstract BooleanSeries getBooleans();
  public abstract StringSeries getStrings();
  public abstract SeriesType type();
  public abstract Series slice(int from, int to);
  public abstract Series sorted();
  public abstract Series copy();
  public abstract Series shift(int offset);
  public abstract boolean hasNull();
  public abstract Series unique();
  public abstract Series append(Series series);

  abstract Series project(int[] fromIndex);
  abstract int[] sortedIndex();
  abstract int[] nullIndex();
  abstract int compare(Series that, int indexThis, int indexThat);

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

  public Series sliceFrom(int from) {
    return this.slice(from, this.size());
  }

  public Series sliceTo(int to) {
    return this.slice(0, to);
  }

  public Series reverse() {
    int[] fromIndex = new int[this.size()];
    for (int i = 0; i < fromIndex.length; i++) {
      fromIndex[i] = fromIndex.length - i - 1;
    }
    return this.project(fromIndex);
  }

  public BooleanSeries map(DoubleConditional conditional) {
    return this.getDoubles().map(conditional);
  }

  public BooleanSeries map(LongConditional conditional) {
    return this.getLongs().map(conditional);
  }

  public BooleanSeries map(StringConditional conditional) {
    return this.getStrings().map(conditional);
  }

  public DoubleSeries map(DoubleFunction function) {
    return this.getDoubles().map(function);
  }

  public LongSeries map(LongFunction function) {
    return this.getLongs().map(function);
  }

  public StringSeries map(StringFunction function) {
    return this.getStrings().map(function);
  }

  public BooleanSeries map(BooleanFunction function) {
    return this.getBooleans().map(function);
  }

  public DoubleSeries aggregate(DoubleFunction function) {
    return this.getDoubles().aggregate(function);
  }

  public LongSeries aggregate(LongFunction function) {
    return this.getLongs().aggregate(function);
  }

  public StringSeries aggregate(StringFunction function) {
    return this.getStrings().aggregate(function);
  }

  public BooleanSeries aggregate(BooleanFunction function) {
    return this.getBooleans().aggregate(function);
  }

  public Series toType(SeriesType type) {
    return DataFrame.asType(this, type);
  }

  public SeriesGrouping groupByValue() {
    if(this.isEmpty())
      return new SeriesGrouping(this);

    List<Bucket> buckets = new ArrayList<>();
    int[] sref = this.sortedIndex();

    int bucketOffset = 0;
    for(int i=1; i<sref.length; i++) {
      if(this.compare(this, sref[i-1], sref[i]) != 0) {
        int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, i);
        buckets.add(new Bucket(fromIndex));
        bucketOffset = i;
      }
    }

    int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, sref.length);
    buckets.add(new Bucket(fromIndex));

    return new SeriesGrouping(this.unique(), this, buckets);
  }

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

  List<JoinPair> join(Series other, JoinType type) {
    // NOTE: merge join
    Series that = other.toType(this.type());

    int[] lref = this.sortedIndex();
    int[] rref = that.sortedIndex();

    List<JoinPair> pairs = new ArrayList<>();
    int i = 0;
    int j = 0;
    while(i < this.size() || j < that.size()) {
      if(j >= that.size() || (i < this.size() && this.compare(that, lref[i], rref[j]) < 0)) {
        switch(type) {
          case LEFT:
          case OUTER:
            pairs.add(new JoinPair(lref[i], -1));
          default:
        }
        i++;
      } else if(i >= this.size() || (j < that.size() && this.compare(that, lref[i], rref[j]) > 0)) {
        switch(type) {
          case RIGHT:
          case OUTER:
            pairs.add(new JoinPair(-1, rref[j]));
          default:
        }
        j++;
      } else if(i < this.size() && j < that.size()) {
        // generate cross product

        // count similar values on the left
        int lcount = 1;
        while(i + lcount < this.size() && this.compare(this, lref[i + lcount], lref[i + lcount - 1]) == 0) {
          lcount++;
        }

        // count similar values on the right
        int rcount = 1;
        while(j + rcount < that.size() && that.compare(that, rref[j + rcount], rref[j + rcount - 1]) == 0) {
          rcount++;
        }

        for(int l=0; l<lcount; l++) {
          for(int r=0; r<rcount; r++) {
            pairs.add(new JoinPair(lref[i + l], rref[j + r]));
          }
        }

        i += lcount;
        j += rcount;
      }
    }

    return pairs;
  }

}
