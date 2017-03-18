package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.NumberUtils;


/**
 * Series container for String objects.
 */
public final class StringSeries extends Series {
  public static final String NULL_VALUE = null;

  // CAUTION: The array is final, but values are inherently modifiable
  final String[] values;

  public static class StringBatchConcat implements StringFunction {
    final String delimiter;

    public StringBatchConcat(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return "";

      StringBuilder builder = new StringBuilder();
      for(int i=0; i<values.length - 1; i++) {
        builder.append(values[i]);
        builder.append(this.delimiter);
      }
      builder.append(values[values.length - 1]);
      return builder.toString();
    }
  }

  public static class StringBatchLast implements StringFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length-1];
    }
  }

  public static class Builder {
    final List<String> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder add(String value) {
      this.values.add(value);
      return this;
    }

    public Builder add(String... values) {
      this.values.addAll(Arrays.asList(values));
      return this;
    }

    public Builder add(Collection<String> values) {
      this.values.addAll(values);
      return this;
    }

    public Builder add(StringSeries series) {
      this.values.addAll(Arrays.asList(series.values));
      return this;
    }

    public StringSeries build() {
      return new StringSeries(values.toArray(new String[values.size()]));
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static StringSeries buildFrom(String... values) {
    return new StringSeries(values);
  }

  public static StringSeries buildFrom(Collection<String> values) {
    return builder().add(values).build();
  }

  public static StringSeries empty() {
    return new StringSeries();
  }

  private StringSeries(String... values) {
    this.values = values;
  }

  @Override
  public DoubleSeries getDoubles() {
    double[] values = new double[this.size()];
    for(int i=0; i<values.length; i++) {
      String value = this.values[i];
      if(StringSeries.isNull(value) || value.length() <= 0) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = Double.parseDouble(value);
      }
    }
    return DoubleSeries.buildFrom(values);
  }

  @Override
  public LongSeries getLongs() {
    long[] values = new long[this.size()];
    for(int i=0; i<values.length; i++) {
      String value = this.values[i];
      if(StringSeries.isNull(value) || value.length() <= 0) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        try {
          values[i] = Long.parseLong(value);
        } catch (NumberFormatException e) {
          values[i] = (long) Double.parseDouble(value);
        }
      }
    }
    return LongSeries.buildFrom(values);
  }

  @Override
  public BooleanSeries getBooleans() {
    byte[] values = new byte[this.size()];
    for(int i=0; i<values.length; i++) {
      String value = this.values[i];
      if(StringSeries.isNull(value) || value.length() <= 0) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        if(NumberUtils.isNumber(value)) {
          values[i] = BooleanSeries.valueOf(Double.parseDouble(value) != 0.0d);
        } else {
          values[i] = BooleanSeries.valueOf(Boolean.parseBoolean(value));
        }
      }
    }
    return BooleanSeries.buildFrom(values);
  }

  @Override
  public StringSeries getStrings() {
    return this;
  }

  @Override
  public StringSeries copy() {
    return StringSeries.buildFrom(Arrays.copyOf(this.values, this.values.length));
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

  @Override
  public StringSeries unique() {
    Set<String> uniques = new HashSet<>(Arrays.asList(this.values));
    String[] values = new String[uniques.size()];
    return StringSeries.buildFrom(uniques.toArray(values));
  }

  /**
   * Returns the contents of the series wrapped as list.
   *
   * @return list of series elements
   */
  public List<String> toList() {
    return Arrays.asList(this.values);
  }

  /**
   * Attempts to infer a tighter native series type based on pattern matching
   * against individual values in the series.
   *
   * @return inferred series type
   */
  public SeriesType inferType() {
    if(this.isEmpty())
      return SeriesType.STRING;

    boolean isBoolean = true;
    boolean isLong = true;
    boolean isDouble = true;

    for(String s : this.values) {
      isBoolean &= (s == null) || (s.length() <= 0) || (s.compareToIgnoreCase("true") == 0 || s.compareToIgnoreCase("false") == 0);
      isLong &= (s == null) || (s.length() <= 0) || (NumberUtils.isNumber(s) && !s.contains(".") && !s.contains("e"));
      isDouble &= (s == null) || (s.length() <= 0) || NumberUtils.isNumber(s);
    }

    if(isBoolean)
      return SeriesType.BOOLEAN;
    if(isLong)
      return SeriesType.LONG;
    if(isDouble)
      return SeriesType.DOUBLE;
    return SeriesType.STRING;
  }

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public String first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
  public String last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public StringSeries slice(int from, int to) {
    return StringSeries.buildFrom(Arrays.copyOfRange(this.values, from, to));
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
  public StringSeries sliceFrom(int from) {
    return (StringSeries)super.sliceFrom(from);
  }

  @Override
  public StringSeries sliceTo(int to) {
    return (StringSeries)super.sliceTo(to);
  }

  @Override
  public StringSeries reverse() {
    return (StringSeries) super.reverse();
  }

  @Override
  public StringSeries sorted() {
    return (StringSeries)super.sorted();
  }

  @Override
  public StringSeries map(StringFunction function) {
    String[] newValues = new String[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i])) {
        newValues[i] = NULL_VALUE;
      } else {
        newValues[i] = function.apply(this.values[i]);
      }
    }
    return StringSeries.buildFrom(newValues);
  }

  @Override
  public BooleanSeries map(StringConditional conditional) {
    byte[] newValues = new byte[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i])) {
        newValues[i] = BooleanSeries.NULL_VALUE;
      } else {
        newValues[i] = BooleanSeries.valueOf(conditional.apply(this.values[i]));
      }
    }
    return BooleanSeries.buildFrom(newValues);
  }

  @Override
  public StringSeries aggregate(StringFunction function) {
    return StringSeries.buildFrom(function.apply(this.values));
  }

  @Override
  public StringSeries append(Series series) {
    String[] values = new String[this.size() + series.size()];
    System.arraycopy(this.values, 0, values, 0, this.size());
    System.arraycopy(series.getStrings().values, 0, values, this.size(), series.size());
    return StringSeries.buildFrom(values);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StringSeries{");
    for(String s : this.values) {
      if(isNull(s)) {
        builder.append("null");
      } else {
        builder.append("'");
        builder.append(s);
        builder.append("' ");
      }
    }
    builder.append("}");
    return builder.toString();
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * <b>value</b>.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public StringSeries fillNull(String value) {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  public StringSeries shift(int offset) {
    String[] values = new String[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL_VALUE);
    }
    return StringSeries.buildFrom(values);
  }

  @Override
  StringSeries project(int[] fromIndex) {
    String[] values = new String[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return StringSeries.buildFrom(values);
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
  int compare(Series that, int indexThis, int indexThat) {
    return nullSafeStringComparator(this.values[indexThis], ((StringSeries)that).values[indexThat]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(String value) {
    return Objects.equals(value, NULL_VALUE);
  }

  private static int nullSafeStringComparator(String a, String b) {
    if (isNull(a) && isNull(b))
      return 0;
    if (isNull(a))
      return -1;
    if (isNull(b))
      return 1;

    return a.compareToIgnoreCase(b);
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
