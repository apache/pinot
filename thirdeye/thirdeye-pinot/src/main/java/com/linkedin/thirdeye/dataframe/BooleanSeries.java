package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.lang.ArrayUtils;


/**
 * Series container for primitive boolean.
 */
public final class BooleanSeries extends Series {
  public static final boolean NULL_VALUE = false;

  // CAUTION: The array is final, but values are inherently modifiable
  final boolean[] values;

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

  public static class Builder {
    final List<Boolean> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder add(boolean value) {
      this.values.add(value);
      return this;
    }

    public Builder add(Boolean value) {
      this.values.add(value);
      return this;
    }

    public Builder add(boolean... values) {
      return this.add(ArrayUtils.toObject(values));
    }

    public Builder add(Boolean... values) {
      this.values.addAll(Arrays.asList(values));
      return this;
    }

    public Builder add(Collection<Boolean> values) {
      this.values.addAll(values);
      return this;
    }

    public BooleanSeries build() {
      return buildFrom(this.values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static BooleanSeries buildFrom(boolean... values) {
    return new BooleanSeries(values);
  }

  public static BooleanSeries buildFrom(Collection<Boolean> values) {
    return new BooleanSeries(ArrayUtils.toPrimitive(
        values.toArray(new Boolean[values.size()])));
  }

  public static BooleanSeries empty() {
    return new BooleanSeries();
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
    double[] values = new double[this.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = this.values[i] ? 1.0d : 0.0d;
    }
    return new DoubleSeries(values);
  }

  @Override
  public LongSeries getLongs() {
    long[] values = new long[this.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = this.values[i] ? 1L : 0L;
    }
    return new LongSeries(values);
  }

  @Override
  public BooleanSeries getBooleans() {
    return this;
  }

  @Override
  public StringSeries getStrings() {
    String[] values = new String[this.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = String.valueOf(this.values[i]);
    }
    return new StringSeries(values);
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

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public boolean first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
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

    if(hasFalse && hasTrue)
      return new BooleanSeries(false, true);
    if(hasFalse)
      return new BooleanSeries(false);
    if(hasTrue)
      return new BooleanSeries(true);
    return new BooleanSeries();
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
