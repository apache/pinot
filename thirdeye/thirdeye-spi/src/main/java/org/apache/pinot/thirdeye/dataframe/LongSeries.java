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
import java.util.List;


/**
 * Series container for primitive long.
 */
public final class LongSeries extends TypedSeries<LongSeries> {
  public static final long NULL = Long.MIN_VALUE;
  public static final long DEFAULT = 0L;
  public static final long MIN_VALUE = Long.MIN_VALUE + 1;
  public static final long MAX_VALUE = Long.MAX_VALUE;

  public static final LongFunction SUM = new LongSum();
  public static final LongFunction PRODUCT = new LongProduct();
  public static final LongFunction FIRST = new LongFirst();
  public static final LongFunction LAST = new LongLast();
  public static final LongFunction MIN = new LongMin();
  public static final LongFunction MAX = new LongMax();

  public static final class LongSum implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL;
      long result = 0;
      for(long v : values)
        result += v;
      return result;
    }
  }

  public static final class LongProduct implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL;
      long result = 1;
      for(long v : values)
        result *= v;
      return result;
    }
  }

  public static final class LongFirst implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL;
      return values[0];
    }
  }

  public static final class LongLast implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length - 1];
    }
  }

  public static final class LongMin implements LongFunction {
    @Override
    public long apply(long[] values) {
      if(values.length <= 0)
        return NULL;
      long min = values[0];
      for(long v : values)
        min = Math.min(min, v);
      return min;
    }
  }

  public static final class LongMax implements LongFunction {
    @Override
    public long apply(long[] values) {
      if (values.length <= 0)
        return NULL;
      long max = values[0];
      for (long v : values)
        max = Math.max(max, v);
      return max;
    }
  }

  public static final class Builder extends Series.Builder {
    final List<long[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(long... values) {
      this.arrays.add(values);
      return this;
    }

    public Builder addValues(long value) {
      return this.addValues(new long[] { value });
    }

    public Builder addValues(Collection<Long> values) {
      long[] newValues = new long[values.size()];
      int i = 0;
      for(Long v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addValues(Long... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Long value) {
      return this.addValues(new long[] { valueOf(value) });
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getLongs().values);
      return this;
    }

    public Builder fillValues(int count, long value) {
      long[] values = new long[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    public Builder fillValues(int count, Long value) {
      return this.fillValues(count, valueOf(value));
    }

    @Override
    public LongSeries build() {
      int totalSize = 0;
      for(long[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      long[] values = new long[totalSize];
      for(long[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return new LongSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static LongSeries buildFrom(long... values) {
    return new LongSeries(values);
  }

  public static LongSeries empty() {
    return new LongSeries();
  }

  public static LongSeries nulls(int size) {
    return builder().fillValues(size, NULL).build();
  }

  public static LongSeries zeros(int size) {
    return builder().fillValues(size, 0L).build();
  }

  public static LongSeries ones(int size) {
    return builder().fillValues(size, 1L).build();
  }

  public static LongSeries fillValues(int size, long value) {
    return builder().fillValues(size, value).build();
  }

  public static LongSeries sequence(long from, int count) {
    return sequence(from, count, 1);
  }

  public static LongSeries sequence(long from, int count, long interval) {
    long[] values = new long[count];
    for(int i=0; i<count; i++) {
      values[i] = from + i * interval;
    }
    return buildFrom(values);
  }

  // CAUTION: The array is final, but values are inherently modifiable
  private final long[] values;

  private LongSeries(long... values) {
    this.values = values;
  }

  @Override
  public Builder getBuilder() {
    return new Builder();
  }

  @Override
  public LongSeries getLongs() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(long value) {
    if(LongSeries.isNull(value))
      return DoubleSeries.NULL;
    return (double) value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(long value) {
    return value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(long value) {
    if(isNull(value))
      return BooleanSeries.NULL;
    return BooleanSeries.valueOf(value != 0L);
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(long value) {
    if(isNull(value))
      return StringSeries.NULL;
    return String.valueOf(value);
  }

  @Override
  public Object getObject(int index) {
    return getObject(this.values[index]);
  }

  public static Object getObject(long value) {
    if(isNull(value))
      return ObjectSeries.NULL;
    return value;
  }

  public long get(int index) {
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
    return SeriesType.LONG;
  }

  public long[] values() {
    return this.values;
  }

  public long value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  /**
   * Returns the contents of the series wrapped as list.
   *
   * @return list of series elements
   */
  public List<Long> toList() {
    Long[] values = new Long[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(!this.isNull(i))
        values[i] = this.values[i];
    }
    return Arrays.asList(values);
  }

  @Override
  public LongSeries slice(int from, int to) {
    from = Math.max(Math.min(this.size(), from), 0);
    to = Math.max(Math.min(this.size(), to), 0);
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("LongSeries{");
    for(long l : this.values) {
      if(isNull(l)) {
        builder.append("null");
      } else {
        builder.append(l);
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString(int index) {
    if(this.isNull(index))
      return TOSTRING_NULL;
    return String.valueOf(this.values[index]);
  }

  public LongSeries sum() {
    return this.aggregate(SUM);
  }

  public LongSeries product() {
    return this.aggregate(PRODUCT);
  }

  public LongSeries min() {
    return this.aggregate(MIN);
  }

  public LongSeries max() {
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

  /**
   * Returns a copy of the series with absolute values ({@code "|x|"}).
   *
   * @return series copy with absolute values
   */
  public LongSeries abs() {
    return this.map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return Math.abs(values[0]);
      }
    });
  }

  public LongSeries add(Series other) {
    if(other.size() == 1)
      return this.add(other.getLong(0));
    return map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] + values[1];
      }
    }, this, other);
  }

  public LongSeries add(final long constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] + constant;
      }
    });
  }

  public LongSeries subtract(Series other) {
    if(other.size() == 1)
      return this.subtract(other.getLong(0));
    return map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] - values[1];
      }
    }, this, other);
  }

  public LongSeries subtract(final long constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] - constant;
      }
    });
  }

  public LongSeries multiply(Series other) {
    if(other.size() == 1)
      return this.multiply(other.getLong(0));
    return map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] * values[1];
      }
    }, this, other);
  }

  public LongSeries multiply(final long constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] * constant;
      }
    });
  }

  public LongSeries divide(Series other) {
    if(other.size() == 1)
      return this.divide(other.getLong(0));
    return map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / values[1];
      }
    }, this, other);
  }

  public LongSeries divide(final long constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / constant;
      }
    });
  }

  public BooleanSeries eq(Series other) {
    if(other.size() == 1)
      return this.eq(other.getLong(0));
    return map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] == values[1];
      }
    }, this, other);
  }

  public BooleanSeries eq(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] == constant;
      }
    });
  }

  public BooleanSeries neq(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map((LongConditional) values -> values[0] != constant);
  }

  public BooleanSeries gt(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] > constant;
      }
    });
  }

  public BooleanSeries gt(Series other) {
    if(other.size() == 1)
      return this.gt(other.getLong(0));
    return map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] > values[1];
      }
    }, this, other);
  }

  public BooleanSeries gte(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= constant;
      }
    });
  }

  public BooleanSeries gte(Series other) {
    if(other.size() == 1)
      return this.gte(other.getLong(0));
    return map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= values[1];
      }
    }, this, other);
  }

  public BooleanSeries lt(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] < constant;
      }
    });
  }

  public BooleanSeries lt(Series other) {
    if(other.size() == 1)
      return this.lt(other.getLong(0));
    return map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] < values[1];
      }
    }, this, other);
  }

  public BooleanSeries lte(final long constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] <= constant;
      }
    });
  }

  public BooleanSeries lte(Series other) {
    if(other.size() == 1)
      return this.lte(other.getLong(0));
    return map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] <= values[1];
      }
    }, this, other);
  }

  public BooleanSeries between(final long startIncl, final long endExcl) {
    if(isNull(startIncl) || isNull(endExcl))
      return BooleanSeries.nulls(this.size());
    return this.map(new LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= startIncl && values[0] < endExcl;
      }
    });
  }

  @Override
  public LongSeries set(BooleanSeries mask, Series other) {
    if(other.size() == 1)
      return this.set(mask, other.getLong(0));
    assertSameLength(this, mask ,other);

    long[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<this.values.length; i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i)))
        values[i] = other.getLong(i);
    }
    return buildFrom(values);
  }

  public LongSeries set(BooleanSeries mask, long value) {
    assertSameLength(this, mask);
    long[] values = new long[this.values.length];
    for(int i=0; i<mask.size(); i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i))) {
        values[i] = value;
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  public int count(long value) {
    int count = 0;
    for(long v : this.values)
      if(v == value)
        count++;
    return count;
  }

  public boolean contains(long value) {
    return this.count(value) > 0;
  }

  public LongSeries replace(long find, long by) {
    if(isNull(find))
      return this.fillNull(by);
    return this.set(this.eq(find), by);
  }

  public int find(long value) {
    return this.find(value, 0);
  }

  public int find(long value, int startOffset) {
    for(int i=startOffset; i<this.values.length; i++)
      if(this.values[i]==value)
        return i;
    return -1;
  }

  @Override
  public LongSeries filter(BooleanSeries filter) {
    return this.set(filter.fillNull().not(), NULL);
  }

  @Override
  public LongSeries fillNull() {
    return this.fillNull(DEFAULT);
  }

  @Override
  int hashCode(int index) {
    return (int) this.values[index];
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public LongSeries fillNull(long value) {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  LongSeries project(int[] fromIndex) {
    long[] values = new long[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
    return buildFrom(values);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongSeries that = (LongSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return Long.compare(this.values[indexThis], that.getLong(indexThat));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static LongSeries map(LongFunction function, Series... series) {
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

    long[] input = new long[series.length];
    long[] output = new long[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static long mapRow(LongFunction function, Series[] series, long[] input, int row) {
    for(int j=0; j<series.length; j++) {
      long value = series[j].getLong(row);
      if(isNull(value))
        return NULL;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static LongSeries mapUnrolled(LongFunction function, Series a) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getLong(i));
      }
    }
    return buildFrom(output);
  }

  private static LongSeries mapUnrolled(LongFunction function, Series a, Series b) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getLong(i), b.getLong(i));
      }
    }
    return buildFrom(output);
  }

  private static LongSeries mapUnrolled(LongFunction function, Series a, Series b, Series c) {
    long[] output = new long[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getLong(i), b.getLong(i), c.getLong(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Function, Series...)
   */
  public static BooleanSeries map(LongConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    assertSameLength(series);

    long[] input = new long[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(LongConditional function, Series[] series, long[] input, int row) {
    for(int j=0; j<series.length; j++) {
      long value = series[j].getLong(row);
      if(isNull(value))
        return BooleanSeries.NULL;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static LongSeries aggregate(LongFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getLongs().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(LongConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getLongs().values)).build();
  }

  public static long valueOf(Long value) {
    if(value == null)
      return NULL;
    return value;
  }

  public static boolean isNull(long value) {
    return value == NULL;
  }

  private static long[] assertNotEmpty(long[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  @Override
  public LongSeries shift(int offset) {
    long[] values = new long[this.values.length];
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
  public LongSeries sorted() {
    long[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);
    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    List<LongSortTuple> tuples = new ArrayList<>();
    for (int i = 0; i < this.values.length; i++) {
      tuples.add(new LongSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<LongSortTuple>() {
      @Override
      public int compare(LongSortTuple a, LongSortTuple b) {
        return Long.compare(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for (int i = 0; i < tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  static final class LongSortTuple {
    final long value;
    final int index;

    LongSortTuple(long value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
