package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public abstract class Series {
  public enum SeriesType {
    DOUBLE,
    LONG,
    STRING,
    BOOLEAN
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

  public static class JoinPair {
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

  public abstract List<Bucket> groupByValue();

  public List<Bucket> groupByCount(int bucketSize) {
    if(bucketSize <= 0)
      throw new IllegalArgumentException("bucketSize must be greater than 0");

    bucketSize = Math.min(bucketSize, this.size());

    List<Bucket> buckets = new ArrayList<>();
    for(int from=0; from<this.size(); from+=bucketSize) {
      int to = Math.min(from+bucketSize, this.size());
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new Bucket(fromIndex));
    }
    return buckets;
  }

  public List<Bucket> groupByPartitions(int partitionCount) {
    if(partitionCount <= 0)
      throw new IllegalArgumentException("partitionCount must be greater than 0");
    if(this.isEmpty())
      return Collections.emptyList();

    double perPartition = this.size() /  (double)partitionCount;

    List<Bucket> buckets = new ArrayList<>();
    for(int i=0; i<partitionCount; i++) {
      int from = (int)Math.round(i * perPartition);
      int to = (int)Math.round((i+1) * perPartition);
      int[] fromIndex = new int[to-from];
      for(int j=0; j<fromIndex.length; j++) {
        fromIndex[j] = j + from;
      }
      buckets.add(new Bucket(fromIndex));
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

  abstract Series project(int[] fromIndex);
  abstract List<JoinPair> joinLeft(Series other);

  abstract int[] sortedIndex();
  abstract int[] nullIndex();
}
