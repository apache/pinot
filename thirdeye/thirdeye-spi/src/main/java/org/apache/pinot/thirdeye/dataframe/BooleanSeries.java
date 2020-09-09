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
import java.util.List;


/**
 * Series container for primitive tri-state boolean (true, false, null). Implementation uses
 * the primitive byte for internal representation.
 */
public final class BooleanSeries extends TypedSeries<BooleanSeries> {
  public static final byte NULL = Byte.MIN_VALUE;
  public static final byte TRUE = 1;
  public static final byte FALSE = 0;
  public static final byte DEFAULT = FALSE;

  public static final BooleanFunctionEx ALL_TRUE = new BooleanAllTrue();
  public static final BooleanFunctionEx HAS_TRUE = new BooleanHasTrue();
  public static final BooleanFunctionEx ALL_FALSE = new BooleanAllFalse();
  public static final BooleanFunctionEx HAS_FALSE = new BooleanHasFalse();
  public static final BooleanFunctionEx FIRST = new BooleanFirst();
  public static final BooleanFunctionEx LAST = new BooleanLast();
  public static final BooleanFunctionEx MIN = ALL_TRUE;
  public static final BooleanFunctionEx MAX = HAS_TRUE;

  public static final class BooleanAllTrue implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      for(byte b : values) {
        if(isFalse(b)) return FALSE;
      }
      return TRUE;
    }
  }

  public static final class BooleanHasTrue implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      for(byte b : values) {
        if(isTrue(b)) return TRUE;
      }
      return FALSE;
    }
  }

  public static final class BooleanAllFalse implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      return valueOf(!isTrue(HAS_TRUE.apply(values)));
    }
  }

  public static final class BooleanHasFalse implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      return valueOf(!isTrue(ALL_TRUE.apply(values)));
    }
  }

  public static final class BooleanFirst implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      return values[0];
    }
  }

  public static final class BooleanLast implements BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length-1];
    }
  }

  public static class Builder extends Series.Builder {
    final List<byte[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(byte... values) {
      byte[] newValues = new byte[values.length];
      for(int i=0; i<values.length; i++) {
        newValues[i] = valueOf(values[i]);
      }
      this.arrays.add(newValues);
      return this;
    }

    public Builder addValues(byte value) {
      return this.addValues(new byte[] { value });
    }

    public Builder addValues(Collection<Byte> values) {
      byte[] newValues = new byte[values.size()];
      int i = 0;
      for(Byte v : values)
        newValues[i++] = valueOf(v);
      this.arrays.add(newValues);
      return this;
    }

    public Builder addValues(Byte... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Byte value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    public Builder addBooleanValues(boolean... values) {
      byte[] newValues = new byte[values.length];
      int i = 0;
      for(boolean v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addBooleanValues(boolean value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    public Builder addBooleanValues(Collection<Boolean> values) {
      byte[] newValues = new byte[values.size()];
      int i = 0;
      for(Boolean v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addBooleanValues(Boolean... values) {
      return this.addBooleanValues(Arrays.asList(values));
    }

    public Builder addBooleanValues(Boolean value) {
      return this.addValues(new byte[] { valueOf(value) });
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getBooleans().values);
      return this;
    }

    public Builder fillValues(int count, byte value) {
      byte[] values = new byte[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    public Builder fillValues(int count, Byte value) {
      return this.fillValues(count, valueOf(value));
    }

    public Builder fillValues(int count, boolean value) {
      return this.fillValues(count, valueOf(value));
    }

    public Builder fillValues(int count, Boolean value) {
      return this.fillValues(count, valueOf(value));
    }

    @Override
    public BooleanSeries build() {
      int totalSize = 0;
      for(byte[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      byte[] values = new byte[totalSize];
      for(byte[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return BooleanSeries.buildFrom(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static BooleanSeries buildFrom(byte... values) {
    return new BooleanSeries(values);
  }

  public static BooleanSeries empty() {
    return new BooleanSeries();
  }

  public static BooleanSeries nulls(int size) {
    return builder().fillValues(size, NULL).build();
  }

  public static BooleanSeries fillValues(int size, byte value) {
    return builder().fillValues(size, value).build();
  }

  public static BooleanSeries fillValues(int size, boolean value) {
    return builder().fillValues(size, value).build();
  }

  // CAUTION: The array is final, but values are inherently modifiable
  private final byte[] values;

  private BooleanSeries(byte... values) {
    this.values = values;
  }

  @Override
  public Builder getBuilder() {
    return new Builder();
  }

  @Override
  public BooleanSeries getBooleans() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(byte value) {
    if(isNull(value))
      return DoubleSeries.NULL;
    return (double) value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(byte value) {
    if(isNull(value))
      return LongSeries.NULL;
    return value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(byte value) {
    return value;
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(byte value) {
    if(isNull(value))
      return StringSeries.NULL;
    return isTrue(value) ? "true" : "false";
  }

  @Override
  public Object getObject(int index) {
    return getObject(this.values[index]);
  }

  public static Object getObject(byte value) {
    if(isNull(value))
      return ObjectSeries.NULL;
    return booleanValueOf(value);
  }

  public byte get(int index) {
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
    return SeriesType.BOOLEAN;
  }

  public byte[] values() {
    return this.values;
  }

  public boolean[] valuesBoolean() {
    boolean[] values = new boolean[this.values.length];
    int i = 0;
    for(byte v : this.values) {
      if(!isNull(v))
        values[i++] = isTrue(v);
    }
    return Arrays.copyOf(values, i);
  }

  public byte value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  public boolean valueBoolean() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return isTrue(this.values[0]);
  }

  /**
   * Returns the contents of the series wrapped as list.
   *
   * @return list of series elements
   */
  public List<Boolean> toList() {
    Boolean[] values = new Boolean[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(!this.isNull(i))
        values[i] = booleanValueOf(this.values[i]);
    }
    return Arrays.asList(values);
  }

  public LongSeries sum() {
    return this.aggregate(LongSeries.SUM);
  }

  public BooleanSeries product() {
    return this.aggregate(ALL_TRUE);
  }

  public BooleanSeries min() {
    return this.aggregate(ALL_TRUE);
  }

  public BooleanSeries max() {
    return this.aggregate(HAS_TRUE);
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

  @Override
  public BooleanSeries slice(int from, int to) {
    from = Math.max(Math.min(this.size(), from), 0);
    to = Math.max(Math.min(this.size(), to), 0);
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  public boolean allTrue() {
    return this.aggregate(ALL_TRUE).valueBoolean();
  }

  public boolean hasTrue() {
    return this.aggregate(HAS_TRUE).valueBoolean();
  }

  public boolean allFalse() {
    return this.aggregate(ALL_FALSE).valueBoolean();
  }

  public boolean hasFalse() {
    return this.aggregate(HAS_FALSE).valueBoolean();
  }

  public BooleanSeries not() {
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(0b1 ^ values[0]);
      }
    });
  }

  public BooleanSeries or(Series other) {
    if(other.size() == 1)
      return this.or(other.getBoolean(0));
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] | values[1]);
      }
    }, this, other);
  }

  public BooleanSeries or(final boolean constant) {
    return this.or(valueOf(constant));
  }

  public BooleanSeries or(final byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] | constant);
      }
    });
  }

  public BooleanSeries and(Series other) {
    if(other.size() == 1)
      return this.and(other.getBoolean(0));
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] & values[1]);
      }
    }, this, other);
  }

  public BooleanSeries and(final boolean constant) {
    return this.and(valueOf(constant));
  }

  public BooleanSeries and(final byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] & constant);
      }
    });
  }

  public BooleanSeries xor(Series other) {
    if(other.size() == 1)
      return this.xor(other.getBoolean(0));
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] ^ values[1]);
      }
    }, this, other);
  }

  public BooleanSeries xor(final boolean constant) {
    return this.xor(valueOf(constant));
  }

  public BooleanSeries xor(final byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)(values[0] ^ constant);
      }
    });
  }

  public BooleanSeries implies(Series other) {
    if(other.size() == 1)
      return this.implies(other.getBoolean(0));
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)((0b1 ^ values[0]) | values[1]);
      }
    }, this, other);
  }

  public BooleanSeries implies(final boolean constant) {
    return this.implies(valueOf(constant));
  }

  public BooleanSeries implies(final byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)((0b1 ^ values[0]) | constant);
      }
    });
  }

  public BooleanSeries eq(Series other) {
    if(other.size() == 1)
      return this.eq(other.getBoolean(0));
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)((values[0] ^ values[1]) ^ 0b1);
      }
    }, this, other);
  }

  public BooleanSeries eq(final boolean constant) {
    return this.eq(valueOf(constant));
  }

  public BooleanSeries eq(final byte constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return (byte)((values[0] ^ constant) ^ 0b1);
      }
    });
  }

  @Override
  public BooleanSeries set(BooleanSeries mask, Series other) {
    if(other.size() == 1)
      return this.set(mask, other.getBoolean(0));
    assertSameLength(this, mask ,other);

    byte[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<this.values.length; i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i)))
        values[i] = other.getBoolean(i);
    }
    return buildFrom(values);
  }

  public BooleanSeries set(BooleanSeries mask, byte value) {
    assertSameLength(this, mask);
    byte[] values = new byte[this.values.length];
    for(int i=0; i<mask.size(); i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i))) {
        values[i] = valueOf(value);
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  public BooleanSeries set(BooleanSeries mask, boolean value) {
    return this.set(mask, valueOf(value));
  }

  public BooleanSeries set(int index, byte value) {
    byte[] values = Arrays.copyOf(this.values, this.values.length);
    values[index] = valueOf(value);
    return buildFrom(values);
  }

  public BooleanSeries set(int index, boolean value) {
    return this.set(index, valueOf(value));
  }

  public int count(boolean value) {
    return this.count(valueOf(value));
  }

  public int count(byte value) {
    int count = 0;
    for(byte v : this.values)
      if(v == valueOf(value))
        count++;
    return count;
  }

  public boolean contains(boolean value) {
    return this.contains(valueOf(value));
  }

  public boolean contains(byte value) {
    return this.count(value) > 0;
  }

  public BooleanSeries replace(boolean find, boolean by) {
    return this.replace(valueOf(find), valueOf(by));
  }

  public BooleanSeries replace(byte find, byte by) {
    if(isNull(find))
      return this.fillNull(by);
    return this.set(this.eq(find), by);
  }

  @Override
  public BooleanSeries filter(BooleanSeries filter) {
    return this.set(filter.fillNull().not(), NULL);
  }

  @Override
  public BooleanSeries unique() {
    boolean hasNull = false;
    boolean hasTrue = false;
    boolean hasFalse = false;

    for(byte v : this.values) {
      hasNull |= isNull(v);
      hasFalse |= isFalse(v);
      hasTrue |= isTrue(v);
    }

    Builder b = builder();
    if(hasNull)
      b.addValues(NULL);
    if(hasFalse)
      b.addValues(FALSE);
    if(hasTrue)
      b.addValues(TRUE);

    return b.build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BooleanSeries{");
    for(byte b : this.values) {
      if(isNull(b)) {
        builder.append("null ");
      } else {
        builder.append(isTrue(b) ? "true " : "false ");
      }
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString(int index) {
    if(isNull(this.values[index]))
      return TOSTRING_NULL;
    if(isFalse(this.values[index]))
      return "false";
    return "true";
  }

  @Override
  public BooleanSeries fillNull() {
    return this.fillNull(DEFAULT);
  }

  /**
   * Return a copy of the series with all {@code null} values replaced by
   * {@code value}.
   *
   * @param value replacement value for {@code null}
   * @return series copy without nulls
   */
  public BooleanSeries fillNull(byte value) {
    byte[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  public BooleanSeries shift(int offset) {
    byte[] values = new byte[this.values.length];
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
  BooleanSeries project(int[] fromIndex) {
    byte[] values = new byte[fromIndex.length];
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
  public BooleanSeries sorted() {
    int countNull = 0;
    int countFalse = 0;
    // countTrue is rest

    for(int i=0; i<this.values.length; i++) {
      if (isNull(this.values[i])) countNull++;
      else if (isFalse(this.values[i])) countFalse++;
    }

    byte[] values = new byte[this.values.length];
    Arrays.fill(values, 0, countNull, NULL);
    Arrays.fill(values, countNull, countNull + countFalse, FALSE);
    Arrays.fill(values, countNull + countFalse, this.values.length, TRUE);

    return buildFrom(values);
  }

  @Override
  int[] sortedIndex() {
    int[] fromIndex = new int[this.values.length];
    int j=0;

    // first null
    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i]))
        fromIndex[j++] = i;
    }

    // then false
    for(int i=0; i<this.values.length; i++) {
      if(isFalse(this.values[i]))
        fromIndex[j++] = i;
    }

    // then true
    for(int i=0; i<this.values.length; i++) {
      if(isTrue(this.values[i]))
        fromIndex[j++] = i;
    }

    return fromIndex;
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
    return Byte.compare(this.values[indexThis], that.getBoolean(indexThat));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  @Override
  int hashCode(int index) {
    return this.values[index];
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(final BooleanFunction function, Series... series) {
    final boolean[] input = new boolean[series.length];
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        for(int i=0; i<input.length; i++) {
          input[i] = booleanValueOf(values[i]);
        }
        return function.apply(input) ? TRUE : FALSE;
      }
    }, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(BooleanFunctionEx function, Series... series) {
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

    byte[] input = new byte[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static byte mapRow(BooleanFunctionEx function, Series[] series, byte[] input, int row) {
    for(int j=0; j<series.length; j++) {
      byte value = series[j].getBoolean(row);
      if(isNull(value))
        return NULL;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static BooleanSeries mapUnrolled(BooleanFunctionEx function, Series a) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getBoolean(i));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries mapUnrolled(BooleanFunctionEx function, Series a, Series b) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getBoolean(i), b.getBoolean(i));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries mapUnrolled(BooleanFunctionEx function, Series a, Series b, Series c) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getBoolean(i), b.getBoolean(i), c.getBoolean(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(final BooleanConditional function, Series... series) {
    return map(new BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return function.apply(values);
      }
    }, series);
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunction function, Series series) {
    return builder().addBooleanValues(function.apply(series.dropNull().getBooleans().valuesBoolean())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunctionEx function, Series series) {
    return builder().addValues(function.apply(series.dropNull().getBooleans().values())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanConditional function, Series series) {
    return builder().addBooleanValues(function.apply(series.dropNull().getBooleans().valuesBoolean())).build();
  }

  public static boolean isNull(byte value) {
    return value == NULL;
  }

  public static boolean isFalse(byte value) {
    return value == FALSE;
  }

  public static boolean isTrue(byte value) {
    return value != NULL && value != FALSE;
  }

  public static byte valueOf(boolean value) {
    return value ? TRUE : FALSE;
  }

  public static byte valueOf(Boolean value) {
    return value == null ? NULL : valueOf(value.booleanValue());
  }

  public static byte valueOf(byte value) {
    return isNull(value) ? NULL : isFalse(value) ? FALSE : TRUE;
  }

  public static byte valueOf(Byte value) {
    return value == null ? NULL : valueOf((byte)value);
  }

  public static boolean booleanValueOf(byte value) {
    return isTrue(value);
  }

  private static byte[] assertNotEmpty(byte[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }
}
