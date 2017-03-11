package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


public final class StringSeries extends Series {
  public static final String NULL_VALUE = null;

  String[] values;

  @FunctionalInterface
  public interface StringFunction {
    String apply(String value);
  }

  @FunctionalInterface
  public interface StringConditional {
    boolean apply(String value);
  }

  public static class StringBatchConcat implements Series.StringBatchFunction {
    final String delimiter;

    public StringBatchConcat(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public String apply(String[] values) {
      StringBuilder builder = new StringBuilder();
      for(int i=0; i<values.length - 1; i++) {
        builder.append(values[i]);
        builder.append(this.delimiter);
      }
      builder.append(values[values.length - 1]);
      return builder.toString();
    }
  }

  public static class StringBatchLast implements Series.StringBatchFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length-1];
    }
  }

  public StringSeries(String... values) {
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
  public StringSeries copy() {
    return new StringSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.STRING;
  }

  public String[] values() {
    return this.values;
  }

  public StringSeries unique() {
    Set<String> uniques = new HashSet<>(Arrays.asList(this.values));
    String[] values = new String[uniques.size()];
    return new StringSeries(uniques.toArray(values));
  }

  public List<String> toList() {
    return Arrays.asList(this.values);
  }

  public String first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public String last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public StringSeries slice(int from, int to) {
    return new StringSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public StringSeries head(int n) {
    return (StringSeries) super.head(n);
  }

  @Override
  public StringSeries tail(int n) {
    return (StringSeries) super.tail(n);
  }

  @Override
  public StringSeries reverse() {
    return (StringSeries) super.reverse();
  }

  public StringSeries map(StringFunction function) {
    assertNotNull();
    return this.mapWithNull(function);
  }

  public StringSeries mapWithNull(StringFunction function) {
    String[] newValues = new String[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new StringSeries(newValues);
  }

  public BooleanSeries map(StringConditional conditional) {
    assertNotNull();
    return this.mapWithNull(conditional);
  }

  public BooleanSeries mapWithNull(StringConditional conditional) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = conditional.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  @Override
  public StringSeries sorted() {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return new StringSeries(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StringSeries{");
    for(String s : this.values) {
      builder.append("'");
      builder.append(s);
      builder.append("' ");
    }
    builder.append("}");
    return builder.toString();
  }

  public StringSeries fillNull(String value) {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return new StringSeries(values);
  }

  @Override
  public StringSeries shift(int offset) {
    String[] values = new String[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), Math.min(-offset, values.length), NULL_VALUE);
    }
    return new StringSeries(values);
  }

  @Override
  StringSeries project(int[] fromIndex) {
    String[] values = new String[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      values[i] = this.values[fromIndex[i]];
    }
    return new StringSeries(values);
  }

  @Override
  int[] sortedIndex() {
    List<StringSortTuple> tuples = new ArrayList<>();
    for(int i=0; i<this.values.length; i++) {
      tuples.add(new StringSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<StringSortTuple>() {
      @Override
      public int compare(StringSortTuple a, StringSortTuple b) {
        if(a.value == null)
          return b.value == null ? 0 : -1;
        return a.value.compareTo(b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for(int i=0; i<tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  @Override
  public boolean hasNull() {
    for(String v : this.values)
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
  List<JoinPair> joinLeft(Series other) {
    return null;
  }

  @Override
  public SeriesGrouping groupByValue() {
    if(this.isEmpty())
      return new SeriesGrouping(this);

    List<String> keys = new ArrayList<>();
    List<Bucket> buckets = new ArrayList<>();
    int[] sortedIndex = this.sortedIndex();

    int bucketOffset = 0;
    String lastValue = this.values[sortedIndex[0]];

    for(int i=1; i<sortedIndex.length; i++) {
      String currVal = this.values[sortedIndex[i]];
      if(!Objects.equals(lastValue, currVal)) {
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

    return new SeriesGrouping(DataFrame.toSeriesFromString(keys), this, buckets);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StringSeries that = (StringSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(String value) {
    return Objects.equals(value, NULL_VALUE);
  }

  private void assertNotNull() {
    if(hasNull())
      throw new IllegalStateException("Must not contain null values");
  }

  private static String[] assertNotEmpty(String[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  static final class StringSortTuple {
    final String value;
    final int index;

    StringSortTuple(String value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
