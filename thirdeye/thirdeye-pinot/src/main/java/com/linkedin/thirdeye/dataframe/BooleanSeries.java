package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public final class BooleanSeries extends Series {
  public static final boolean NULL_VALUE = false;

  boolean[] values;

  public static class BooleanBatchAnd implements BooleanFunction {
    @Override
    public boolean apply(boolean[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      for(boolean b : values) {
        if(!b)
          return false;
      }
      return true;
    }
  }

  public static class BooleanBatchOr implements BooleanFunction {
    @Override
    public boolean apply(boolean[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      for(boolean b : values) {
        if(b)
          return true;
      }
      return false;
    }
  }

  public static class BooleanBatchLast implements BooleanFunction {
    @Override
    public boolean apply(boolean[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length-1];
    }
  }

  BooleanSeries(boolean... values) {
    this.values = values;
  }

  @Override
  public BooleanSeries copy() {
    return new BooleanSeries(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries getDoubles() {
    return DataFrame.getDoubles(this);
  }

  @Override
  public LongSeries getLongs() {
    return DataFrame.getLongs(this);
  }

  @Override
  public BooleanSeries getBooleans() {
    return DataFrame.toBooleans(this);
  }

  @Override
  public StringSeries getStrings() {
    return DataFrame.getStrings(this);
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.BOOLEAN;
  }

  public boolean[] values() {
    return this.values;
  }

  public boolean first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  public boolean last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public BooleanSeries slice(int from, int to) {
    return new BooleanSeries(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public BooleanSeries head(int n) {
    return (BooleanSeries) super.head(n);
  }

  @Override
  public BooleanSeries tail(int n) {
    return (BooleanSeries) super.tail(n);
  }

  @Override
  public BooleanSeries sliceFrom(int from) {
    return (BooleanSeries)super.sliceFrom(from);
  }

  @Override
  public BooleanSeries sliceTo(int to) {
    return (BooleanSeries)super.sliceTo(to);
  }

  @Override
  public BooleanSeries reverse() {
    return (BooleanSeries)super.reverse();
  }

  @Override
  public BooleanSeries map(BooleanFunction function) {
    boolean[] newValues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return new BooleanSeries(newValues);
  }

  @Override
  public BooleanSeries aggregate(BooleanFunction function) {
    return new BooleanSeries(function.apply(this.values));
  }

  @Override
  public BooleanSeries append(Series series) {
    boolean[] values = new boolean[this.size() + series.size()];
    System.arraycopy(this.values, 0, values, 0, this.size());
    System.arraycopy(series.getBooleans().values, 0, values, this.size(), series.size());
    return new BooleanSeries(values);
  }

  public boolean allTrue() {
    assertNotEmpty(this.values);
    boolean result = true;
    for(boolean b : this.values) {
      result &= b;
    }
    return result;
  }

  public boolean hasTrue() {
    boolean result = false;
    for(boolean b : this.values) {
      result |= b;
    }
    return result;
  }

  public boolean allFalse() {
    assertNotEmpty(this.values);
    boolean result = true;
    for(boolean b : this.values) {
      result &= !b;
    }
    return result;
  }

  public boolean hasFalse() {
    boolean result = false;
    for(boolean b : this.values) {
      result |= !b;
    }
    return result;
  }

  public BooleanSeries not() {
    boolean[] bvalues = new boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      bvalues[i] = !this.values[i];
    }
    return new BooleanSeries(bvalues);
  }

  @Override
  public BooleanSeries sorted() {
    boolean[] values = new boolean[this.values.length];
    int count_false = 0;

    // count true
    for(boolean b : this.values) {
      if(!b)
        count_false++;
    }

    // first false, then true
    Arrays.fill(values, 0, count_false, false);
    Arrays.fill(values, count_false, values.length, true);

    return new BooleanSeries(values);
  }

  @Override
  public BooleanSeries unique() {
    boolean hasTrue = this.hasTrue();
    boolean hasFalse = this.hasFalse();

    List<Boolean> values = new ArrayList<>();
    if(hasFalse)
      values.add(false);
    if(hasTrue)
      values.add(true);

    return DataFrame.toSeriesFromBoolean(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BooleanSeries{");
    for(boolean b : this.values) {
      builder.append(b);
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public BooleanSeries shift(int offset) {
    boolean[] values = new boolean[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL_VALUE);
    }
    return new BooleanSeries(values);
  }

  @Override
  BooleanSeries project(int[] fromIndex) {
    boolean[] values = new boolean[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return new BooleanSeries(values);
  }

  @Override
  int[] sortedIndex() {
    int[] fromIndex = new int[this.values.length];
    int j=0;

    // false first
    for(int i=0; i<this.values.length; i++) {
      if(!this.values[i])
        fromIndex[j++] = i;
    }

    // then true
    for(int i=0; i<this.values.length; i++) {
      if(this.values[i])
        fromIndex[j++] = i;
    }

    return fromIndex;
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  int[] nullIndex() {
    return new int[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BooleanSeries that = (BooleanSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return Boolean.compare(this.values[indexThis], ((BooleanSeries)that).values[indexThat]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  private static boolean[] assertNotEmpty(boolean[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

}
