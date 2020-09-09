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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.reflect.FieldUtils;


public class ObjectSeries extends TypedSeries<ObjectSeries> {
  public static final String METHOD_DOUBLE = "doubleValue";
  public static final String METHOD_DOUBLE_FROM_FLOAT = "floatValue";
  public static final String METHOD_LONG = "longValue";
  public static final String METHOD_LONG_FROM_INT = "intValue";
  public static final String METHOD_BOOLEAN = "booleanValue";
  public static final String METHOD_STRING = "toString";
  public static final String METHOD_COMPARE = "compareTo";

  public static final ObjectFunction FIRST = new ObjectFirst();
  public static final ObjectFunction LAST = new ObjectLast();
  public static final ObjectFunction MIN = new ObjectMin();
  public static final ObjectFunction MAX = new ObjectMax();
  public static final ObjectFunction TOSTRING = new ObjectToString();

  public static final Object NULL = null;

  public static final class ObjectFirst implements ObjectFunction {
    @Override
    public Object apply(Object[] values) {
      if(values.length <= 0)
        return NULL;
      return values[0];
    }
  }

  public static final class ObjectLast implements ObjectFunction {
    @Override
    public Object apply(Object[] values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length - 1];
    }
  }

  public static final class ObjectMin implements ObjectFunction {
    @Override
    public Object apply(Object[] values) {
      if(values.length <= 0)
        return NULL;
      return Collections.min(Arrays.asList(values), new Comparator<Object>() {
        @Override
        public int compare(Object o1, Object o2) {
          return nullSafeObjectComparator(o1, o2);
        }
      });
    }
  }

  public static final class ObjectMax implements ObjectFunction {
    @Override
    public Object apply(Object[] values) {
      if(values.length <= 0)
        return NULL;
      return Collections.max(Arrays.asList(values), new Comparator<Object>() {
        @Override
        public int compare(Object o1, Object o2) {
          return nullSafeObjectComparator(o1, o2);
        }
      });
    }
  }

  public static final class ObjectToString implements ObjectFunction {
    @Override
    public Object apply(Object[] values) {
      if(values.length <= 0)
        return "[]";
      return "[" + StringUtils.join(values, ", ") + "]";
    }
  }

  public static final class Builder extends Series.Builder {
    final List<Object[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(Object... values) {
      this.arrays.add(values);
      return this;
    }

    public Builder addValues(Collection<?> values) {
      return this.addValues(values.toArray());
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getObjects().values);
      return this;
    }

    public Builder fillValues(int count, Object value) {
      Object[] values = new Object[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    @Override
    public ObjectSeries build() {
      int totalSize = 0;
      for(Object[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      Object[] values = new Object[totalSize];
      for(Object[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return new ObjectSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ObjectSeries buildFrom(Object... values) {
    return new ObjectSeries(values);
  }

  public static ObjectSeries empty() {
    return new ObjectSeries();
  }

  public static ObjectSeries nulls(int n) {
    return builder().fillValues(n, NULL).build();
  }

  public static ObjectSeries fillValues(int n, Object value) {
    return builder().fillValues(n, value).build();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  private final Object[] values;

  private ObjectSeries(Object... values) {
    this.values = values;
  }

  public Object[] values() {
    return this.values;
  }

  public Object value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  @Override
  public int size() {
    return this.values.length;
  }

  @Override
  public SeriesType type() {
    return SeriesType.OBJECT;
  }

  @Override
  public ObjectSeries slice(int from, int to) {
    from = Math.max(Math.min(this.size(), from), 0);
    to = Math.max(Math.min(this.size(), to), 0);
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(Object value) {
    if(isNull(value))
      return DoubleSeries.NULL;
    if(value instanceof Boolean)
      return BooleanSeries.getDouble(BooleanSeries.valueOf((Boolean)value));
    if(value instanceof Number)
      return DoubleSeries.getDouble(((Number)value).doubleValue());
    if(value instanceof String)
      return StringSeries.getDouble(value.toString());

    try {
      return (double) invokeMethod(value, METHOD_DOUBLE);
    } catch (Exception ignore) {
      // ignore
    }

    try {
      return (double) invokeMethod(value, METHOD_DOUBLE_FROM_FLOAT);
    } catch (Exception ignore) {
      // ignore
    }

    throw new IllegalArgumentException(String.format("Cannot convert object '%s' to double", value.toString()));
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(Object value) {
    if(isNull(value))
      return LongSeries.NULL;
    if(value instanceof Boolean)
      return BooleanSeries.getLong(BooleanSeries.valueOf((Boolean)value));
    if(value instanceof Number)
      return DoubleSeries.getLong(((Number)value).doubleValue());
    if(value instanceof String)
      return StringSeries.getLong(value.toString());

    try {
      return (long) invokeMethod(value, METHOD_LONG);
    } catch (Exception ignore) {
      // ignore
    }

    try {
      return (long) invokeMethod(value, METHOD_LONG_FROM_INT);
    } catch (Exception ignore) {
      // ignore
    }

    throw new IllegalArgumentException(String.format("Cannot convert object '%s' to long", value.toString()));
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(Object value) {
    if(isNull(value))
      return BooleanSeries.NULL;
    if(value instanceof Boolean)
      return BooleanSeries.valueOf((Boolean)value);
    if(value instanceof Number)
      return DoubleSeries.getBoolean(((Number)value).doubleValue());
    if(value instanceof String)
      return StringSeries.getBoolean(value.toString());
    try {
      return BooleanSeries.valueOf((boolean) invokeMethod(value, METHOD_BOOLEAN));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(Object value) {
    if(isNull(value))
      return StringSeries.NULL;
    return value.toString();
  }

  @Override
  public Object getObject(int index) {
    return getObject(this.values[index]);
  }

  public static Object getObject(Object value) {
    return value;
  }

  @Override
  public ObjectSeries getObjects() {
    return this;
  }

  public Object get(int index) {
    return this.values[index];
  }

  public List<Object> toList() {
    return this.toListTyped();
  }

  @SuppressWarnings("unchecked")
  public <T> T getObjectTyped(int index) {
    return (T) getObject(this.values[index]);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> toListTyped() {
    return (List<T>)Arrays.asList(this.values);
  }

  @Override
  public boolean isNull(int index) {
    return isNull(this.values[index]);
  }

  public static boolean isNull(Object value) {
    return value == NULL;
  }

  @Override
  public String toString(int index) {
    if(this.isNull(index))
      return TOSTRING_NULL;
    return String.valueOf(this.values[index]);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ObjectSeries{");
    for(Object o : this.values) {
      if(isNull(o)) {
        builder.append("null");
      } else {
        builder.append(o.toString());
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public ObjectSeries sorted() {
    Object[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values, new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        return nullSafeObjectComparator(o1, o2);
      }
    });
    return buildFrom(values);
  }

  public ObjectSeries sorted(Comparator comparator) {
    Object[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values, comparator);
    return buildFrom(values);
  }

  @Override
  public ObjectSeries unique() {
    HashSet<Object> objects = new LinkedHashSet<>();
    objects.addAll(Arrays.asList(this.values));
    return buildFrom(objects.toArray());
  }

  @Override
  public ObjectSeries fillNull() {
    return this;
  }

  @Override
  public Builder getBuilder() {
    return builder();
  }

  @Override
  public ObjectSeries min() {
    return this.aggregate(MIN);
  }

  @Override
  public ObjectSeries max() {
    return this.aggregate(MAX);
  }

  public BooleanSeries eq(Series other) {
    if(other.size() == 1)
      return this.eq(other.getObject(0));
    return map(new ObjectConditional() {
      @Override
      public boolean apply(Object... values) {
        return nullSafeObjectEquals(values[0], values[1]);
      }
    }, this, other);
  }

  public BooleanSeries eq(final Object constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new ObjectConditional() {
      @Override
      public boolean apply(Object... values) {
        return nullSafeObjectEquals(values[0], constant);
      }
    });
  }

  @Override
  public ObjectSeries set(BooleanSeries mask, Series other) {
    if(other.size() == 1)
      return this.set(mask, other.getObject(0));
    assertSameLength(this, mask ,other);

    Object[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<this.values.length; i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i)))
        values[i] = other.getObject(i);
    }
    return buildFrom(values);
  }

  public ObjectSeries set(BooleanSeries mask, Object value) {
    assertSameLength(this, mask);
    Object[] values = new Object[this.values.length];
    for(int i=0; i<mask.size(); i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i))) {
        values[i] = value;
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  public int count(Object value) {
    int count = 0;
    for(Object v : this.values)
      if(v == value)
        count++;
    return count;
  }

  public boolean contains(Object value) {
    return this.count(value) > 0;
  }

  public ObjectSeries replace(Object find, Object by) {
    if(isNull(find))
      return this.fillNull(by);
    return this.set(this.eq(find), by);
  }

  @Override
  public ObjectSeries filter(BooleanSeries filter) {
    return this.set(filter.fillNull().not(), NULL);
  }

  @Override
  ObjectSeries project(int[] fromIndex) {
    Object[] values = new Object[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return buildFrom(values);
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public ObjectSeries fillNull(Object value) {
    Object[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  public SeriesType inferType() {
    if(this.isEmpty())
      return SeriesType.OBJECT;

    boolean isBoolean = true;
    boolean isLong = true;
    boolean isDouble = true;

    for(int i=0; i<this.size(); i++) {
      Object o = this.getObject(i);
      if(o == null)
        continue;

      if(!(o instanceof Number) && !(o instanceof String) && !(o instanceof Boolean))
        return Series.SeriesType.OBJECT;

      if(o instanceof Boolean) {
        isDouble = false;
        isLong = false;
      }

      if(o instanceof Number) {
        isBoolean = false;
        isLong &= ((Number)o).longValue() == ((Number)o).doubleValue();
      }

      if(o instanceof String) {
        String s = o.toString();
        isBoolean &= (s.length() <= 0) || (s.compareToIgnoreCase("true") == 0 || s.compareToIgnoreCase("false") == 0);
        isLong &= (s.length() <= 0) || (NumberUtils.isNumber(s) && !s.contains(".") && !s.contains("e"));
        isDouble &= (s.length() <= 0) || NumberUtils.isNumber(s);
      }
    }

    if(isBoolean)
      return Series.SeriesType.BOOLEAN;
    if(isLong)
      return Series.SeriesType.LONG;
    if(isDouble)
      return Series.SeriesType.DOUBLE;
    return Series.SeriesType.STRING;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ObjectSeries that = (ObjectSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return nullSafeObjectComparator(this.values[indexThis], that.getObject(indexThat));
  }

  @Override
  boolean equals(Series that, int indexThis, int indexThat) {
    return nullSafeObjectEquals(this.values[indexThis], that.getObject(indexThat));
  }

  private static int nullSafeObjectComparator(Object a, Object b) {
    if(isNull(a) && isNull(b))
      return 0;
    if(isNull(a))
      return -1;
    if(isNull(b))
      return 1;
    try {
      return (int) invokeMethod(a, METHOD_COMPARE, b);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean nullSafeObjectEquals(Object a, Object b) {
    if(isNull(a) && isNull(b))
      return true;
    if(isNull(a))
      return false;
    if(isNull(b))
      return false;
    return a.equals(b);
  }

  private static Object invokeMethod(Object o, String name, Object... args) {
    try {
      Class<?>[] argTypes = new Class<?>[args.length];
      Arrays.fill(argTypes, Object.class);
      Method m = o.getClass().getMethod(name, argTypes);
      return m.invoke(o, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectSeries shift(int offset) {
    Object[] values = new Object[this.values.length];
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
  int[] sortedIndex() {
    List<ObjectSortTuple> tuples = new ArrayList<>();
    for (int i = 0; i < this.values.length; i++) {
      tuples.add(new ObjectSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<ObjectSortTuple>() {
      @Override
      public int compare(ObjectSortTuple a, ObjectSortTuple b) {
        return nullSafeObjectComparator(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for (int i = 0; i < tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  @Override
  int hashCode(int index) {
    return this.values[index].hashCode();
  }

  static final class ObjectSortTuple {
    final Object value;
    final int index;

    ObjectSortTuple(Object value, int index) {
      this.value = value;
      this.index = index;
    }
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static ObjectSeries map(ObjectFunction function, Series... series) {
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

    Object[] input = new Object[series.length];
    Object[] output = new Object[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static Object mapRow(ObjectFunction function, Series[] series, Object[] input, int row) {
    for(int j=0; j<series.length; j++) {
      Object value = series[j].getObject(row);
      if(isNull(value))
        return NULL;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static ObjectSeries mapUnrolled(ObjectFunction function, Series a) {
    Object[] output = new Object[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getObject(i));
      }
    }
    return buildFrom(output);
  }

  private static ObjectSeries mapUnrolled(ObjectFunction function, Series a, Series b) {
    Object[] output = new Object[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getObject(i), b.getObject(i));
      }
    }
    return buildFrom(output);
  }

  private static ObjectSeries mapUnrolled(ObjectFunction function, Series a, Series b, Series c) {
    Object[] output = new Object[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getObject(i), b.getObject(i), c.getObject(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static BooleanSeries map(ObjectConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    assertSameLength(series);

    Object[] input = new Object[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(ObjectConditional function, Series[] series, Object[] input, int row) {
    for(int j=0; j<series.length; j++) {
      Object value = series[j].getObject(row);
      if(isNull(value))
        return BooleanSeries.NULL;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static ObjectSeries aggregate(ObjectFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getObjects().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(ObjectConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getObjects().values)).build();
  }

  /**
   * Returns an object series of values extracted from the original values in the object series.
   * Allows field and method access to nested objects, with names separated by {@code "."} (dot).
   *
   * <br/><b>EXAMPLE:</b> {@code "this.myMethod().myResultField"}
   *
   * <br/><b>NOTE:</b> only no-args methods are supported
   *
   * @param objectExpression expression to extract values with
   * @return series of extracted values
   */
  public ObjectSeries map(String objectExpression) {
    return map(objectExpression, this);
  }

  public static ObjectSeries map(String objectExpression, Series... series) {
    if(series.length <= 0)
      return empty();
    if(series.length > 1)
      throw new IllegalArgumentException("Must provide at most 1 series");

    DataFrame.assertSameLength(series);

    // TODO support escaping of "."
    List<String> expressions = Arrays.asList(objectExpression.split("\\."));
    if(expressions.isEmpty())
      return ObjectSeries.nulls(series[0].size());

    try {
      Object[] out = new Object[series[0].size()];
      for(int i=0; i<series[0].size(); i++) {
        if(series[0].isNull(i)) {
          out[i] = NULL;
        } else {
          out[i] = mapExpression(expressions, series[0].getObject(i));
        }
      }

      return buildFrom(out);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Object mapExpression(List<String> expressions, Object value) throws Exception {
    if(expressions.isEmpty())
      return value;

    String head = expressions.get(0);
    List<String> tail = expressions.subList(1, expressions.size());

    if(head.equals("this"))
      return mapExpression(tail, value);

    if(head.endsWith("()")) {
      head = head.substring(0, head.length() - 2);
      // method call
      return mapExpression(tail, invokeMethod(value, head));

    } else {
      // field access
      return mapExpression(tail, FieldUtils.readField(value, head, true));
    }
  }
}
