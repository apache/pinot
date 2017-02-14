package com.linkedin.thirdeye.detector.functionex.dataframe;

public abstract class Series<T extends Series> {
  public abstract int size();
  public abstract DoubleSeries toDoubles();
  public abstract LongSeries toLongs();
  public abstract BooleanSeries toBooleans();
  public abstract StringSeries toStrings();
  public abstract SeriesType type();
  public abstract T slice(int from, int to);
  public T head(int n) {
    int len = this.size();
    return this.slice(0, len);
  }
  public T tail(int n) {
    int len = this.size();
    return this.slice(len - n, len);
  }

  public abstract T sort();
  public abstract T reorder(int[] toIndex);

  public T reverse() {
    int[] toIndex = new int[this.size()];
    for (int i = 0; i < toIndex.length; i++) {
      toIndex[i] = toIndex.length - i - 1;
    }
    return this.reorder(toIndex);
  }

  abstract int[] sortedIndex();

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
      return fromIndex.length;
    }
  }
}
