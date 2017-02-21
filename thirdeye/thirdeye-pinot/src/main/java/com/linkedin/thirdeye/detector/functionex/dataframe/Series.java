package com.linkedin.thirdeye.detector.functionex.dataframe;

public abstract class Series {
  public abstract int size();
  public abstract DoubleSeries toDoubles();
  public abstract LongSeries toLongs();
  public abstract BooleanSeries toBooleans();
  public abstract StringSeries toStrings();
  public abstract SeriesType type();
  public abstract Series slice(int from, int to);
  public Series head(int n) {
    int len = this.size();
    return this.slice(0, len);
  }
  public Series tail(int n) {
    int len = this.size();
    return this.slice(len - n, len);
  }

  public abstract Series sort();
  public abstract Series reorder(int[] toIndex);

  public Series reverse() {
    int[] toIndex = new int[this.size()];
    for (int i = 0; i < toIndex.length; i++) {
      toIndex[i] = toIndex.length - i - 1;
    }
    return this.reorder(toIndex);
  }

  abstract int[] sortedIndex();
  abstract Series filter(int[] fromIndex);

  public enum SeriesType {
    DOUBLE,
    LONG,
    STRING,
    BOOLEAN
  }

  public static class Bucket {
    final int[] fromIndex;

    public Bucket(int[] fromIndex) {
      this.fromIndex = fromIndex;
    }

    public int size() {
      return this.fromIndex.length;
    }
  }
}
