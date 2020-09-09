/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.math.NumberUtils;


/**
 * Series container for String objects.
 */
public final class StringSeries extends TypedSeries<StringSeries> {
  public static final String NULL = null;
  public static final String DEFAULT = "";

  public static final StringFunction CONCAT = new StringConcat();
  public static final StringFunction FIRST = new StringFirst();
  public static final StringFunction LAST = new StringLast();
  public static final StringFunction MIN = new StringMin();
  public static final StringFunction MAX = new StringMax();

  public static final class StringConcat implements StringFunction {
    final String delimiter;

    public StringConcat() {
      this.delimiter = "";
    }

    public StringConcat(String delimiter) {
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

  public static final class StringFirst implements StringFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL;
      return values[0];
    }
  }

  public static final class StringLast implements StringFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length-1];
    }
  }

  public static final class StringMin implements StringFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL;
      return Collections.min(Arrays.asList(values));
    }
  }

  public static final class StringMax implements StringFunction {
    @Override
    public String apply(String[] values) {
      if(values.length <= 0)
        return NULL;
      return Collections.max(Arrays.asList(values));
    }
  }

  public static class Builder extends Series.Builder {
    final List<String> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(Collection<String> values) {
      this.values.addAll(values);
      return this;
    }

    public Builder addValues(String... values) {
      return this.addValues(Arrays.asList(values));
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getStrings().values);
      return this;
    }

    public Builder fillValues(int count, String value) {
      String[] values = new String[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    @Override
    public StringSeries build() {
      return StringSeries.buildFrom(this.values.toArray(new String[this.values.size()]));
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static StringSeries buildFrom(String... values) {
    return new StringSeries(values);
  }

  public static StringSeries empty() {
    return new StringSeries();
  }

  public static StringSeries nulls(int size) {
    return builder().fillValues(size, NULL).build();
  }

  public static StringSeries fillValues(int size, String value) {
    return builder().fillValues(size, value).build();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  private final String[] values;

  private StringSeries(String... values) {
    this.values = values;
  }

  @Override
  public Builder getBuilder() {
    return new Builder();
  }

  @Override
  public StringSeries getStrings() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(String value) {
    if(StringSeries.isNull(value) || value.length() <= 0)
      return DoubleSeries.NULL;
    return Double.parseDouble(value);
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(String value) {
    if(StringSeries.isNull(value) || value.length() <= 0)
      return LongSeries.NULL;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return (long) Double.parseDouble(value);
    }
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(String value) {
    if(StringSeries.isNull(value) || value.length() <= 0)
      return BooleanSeries.NULL;
    if(NumberUtils.isNumber(value))
      return BooleanSeries.valueOf(Double.parseDouble(value) != 0.0d);
    return BooleanSeries.valueOf(Boolean.parseBoolean(value));
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(String string) {
    return string;
  }

  @Override
  public Object getObject(int index) {
    return getObject(this.values[index]);
  }

  public static Object getObject(String value) {
    if(isNull(value))
      return ObjectSeries.NULL;
    return value;
  }

  public String get(int index) {
    return this.values[index];
  }

  @Override
  public boolean isNull(int index) {
    return isNull(this.values[index]);
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

  public String value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  @Override
  public StringSeries unique() {
    Set<String> uniques = new HashSet<>(Arrays.asList(this.values));
    String[] values = new String[uniques.size()];
    return StringSeries.buildFrom(uniques.toArray(values));
  }

  /**
   * Returns a compressed series via string de-duplication. After applying this method,
   * equal strings in the series reference the same string instance.
   *
   * @return compressed string series
   */
  public StringSeries compress() {
    Map<String, String> map = new HashMap<>();

    String[] values = new String[this.values.length];
    for (int i = 0; i < values.length; i++) {
      String v = this.values[i];
      if (!map.containsKey(v)) {
        map.put(v, v);
      }
      values[i] = map.get(v);
    }

    return buildFrom(values);
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
   * Attempts to infer a tighter native series type based on pattern matching against individual
   * values in the series. Returns a copy of the series with the inferred type.
   *
   * @return series copy of inferred type
   */
  public Series toInferredType() {
    return this.get(this.inferType());
  }

  @Override
  public StringSeries slice(int from, int to) {
    from = Math.max(Math.min(this.size(), from), 0);
    to = Math.max(Math.min(this.size(), to), 0);
    return StringSeries.buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  // TODO validate design decision
  public StringSeries sum() {
    return this.aggregate(CONCAT);
  }

  public DoubleSeries product() {
    return this.aggregate(DoubleSeries.PRODUCT);
  }

  public StringSeries min() {
    return this.aggregate(MIN);
  }

  public StringSeries max() {
    return this.aggregate(MAX);
  }

  public DoubleSeries mean() {
    return this.aggregate(DoubleSeries.MEAN);
  }

  public DoubleSeries median() {
    return this.aggregate(DoubleSeries.MEDIAN);
  }

  public DoubleSeries std() {
    return this.aggregate(DoubleSeries.STD);
  }

  public String join() {
    return this.aggregate(CONCAT).value();
  }

  public String join(String delimiter) {
    return this.aggregate(new StringConcat(delimiter)).value();
  }

  public StringSeries concat(Series other) {
    if(other.size() == 1)
      return this.concat(other.getString(0));
    return map(new StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + values[1];
      }
    }, this, other);
  }

  public StringSeries concat(final String constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + constant;
      }
    });
  }

  public BooleanSeries eq(Series other) {
    if(other.size() == 1)
      return this.eq(other.getString(0));
    return map(new StringConditional() {
      @Override
      public boolean apply(String... values) {
        return values[1].equals(values[0]);
      }
    }, this, other);
  }

  public BooleanSeries eq(final String constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new StringConditional() {
      @Override
      public boolean apply(String... values) {
        return constant.equals(values[0]);
      }
    });
  }

  @Override
  public StringSeries set(BooleanSeries mask, Series other) {
    if(other.size() == 1)
      return this.set(mask, other.getString(0));
    assertSameLength(this, mask ,other);

    String[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<this.values.length; i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i)))
        values[i] = other.getString(i);
    }
    return buildFrom(values);
  }

  public StringSeries set(BooleanSeries mask, String value) {
    assertSameLength(this, mask);
    String[] values = new String[this.values.length];
    for(int i=0; i<mask.size(); i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i))) {
        values[i] = value;
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  public int count(String value) {
    int count = 0;
    for(String v : this.values)
      if(nullSafeStringComparator(v, value) == 0)
        count++;
    return count;
  }

  public boolean contains(String value) {
    return this.count(value) > 0;
  }

  public StringSeries replace(String find, String by) {
    if(isNull(find))
      return this.fillNull(by);
    return this.set(this.eq(find), by);
  }

  @Override
  public StringSeries filter(BooleanSeries filter) {
    return this.set(filter.fillNull().not(), NULL);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StringSeries{");
    for(String s : this.values) {
      if(isNull(s)) {
        builder.append("null ");
      } else {
        builder.append("'");
        builder.append(s);
        builder.append("' ");
      }
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString(int index) {
    if(this.isNull(index))
      return TOSTRING_NULL;
    return this.values[index];
  }

  @Override
  public StringSeries fillNull() {
    return this.fillNull(DEFAULT);
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
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
  StringSeries project(int[] fromIndex) {
    String[] values = new String[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return StringSeries.buildFrom(values);
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
    return nullSafeStringComparator(this.values[indexThis], that.getString(indexThat));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  @Override
  int hashCode(int index) {
    return Objects.hashCode(this.values[index]);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static StringSeries map(StringFunction function, Series... series) {
    if(series.length <= 0)
      return empty();

    assertSameLength(series);

    // Note: code-specialization to help hot-spot vm
    if(series.length == 1)
      return mapUnrolled(function, series[0]);
    if(series.length == 2)
      return mapUnrolled(function, series[0], series[1]);
    if(series.length == 3)
      return mapUnrolled(function, series[0], series[1], series[2]);

    String[] input = new String[series.length];
    String[] output = new String[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static String mapRow(StringFunction function, Series[] series, String[] input, int row) {
    for(int j=0; j<series.length; j++) {
      String value = series[j].getString(row);
      if(isNull(value))
        return NULL;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static StringSeries mapUnrolled(StringFunction function, Series a) {
    String[] output = new String[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getString(i));
      }
    }
    return buildFrom(output);
  }

  private static StringSeries mapUnrolled(StringFunction function, Series a, Series b) {
    String[] output = new String[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getString(i), b.getString(i));
      }
    }
    return buildFrom(output);
  }

  private static StringSeries mapUnrolled(StringFunction function, Series a, Series b, Series c) {
    String[] output = new String[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getString(i), b.getString(i), c.getString(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(StringConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    assertSameLength(series);

    String[] input = new String[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(StringConditional function, Series[] series, String[] input, int row) {
    for(int j=0; j<series.length; j++) {
      String value = series[j].getString(row);
      if(isNull(value))
        return BooleanSeries.NULL;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static StringSeries aggregate(StringFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getStrings().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(StringConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getStrings().values)).build();
  }

  public static boolean isNull(String value) {
    return Objects.equals(value, NULL);
  }

  private static int nullSafeStringComparator(String a, String b) {
    if (isNull(a) && isNull(b))
      return 0;
    if (isNull(a))
      return -1;
    if (isNull(b))
      return 1;

    return a.compareTo(b);
  }

  private static String[] assertNotEmpty(String[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  @Override
  public StringSeries shift(int offset) {
    String[] values = new String[this.values.length];
    if(offset >= 0) {
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL);
    }
    return buildFrom(values);
  }

  @Override
  public StringSeries sorted() {
    String[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values, new Comparator<String>() {
      @Override
      public int compare(String a, String b) {
        return nullSafeStringComparator(a, b);
      }
    });
    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    List<StringSortTuple> tuples = new ArrayList<>();
    for (int i = 0; i < this.values.length; i++) {
      tuples.add(new StringSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<StringSortTuple>() {
      @Override
      public int compare(StringSortTuple a, StringSortTuple b) {
        return nullSafeStringComparator(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for (int i = 0; i < tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
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
