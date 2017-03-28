package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.lang.ArrayUtils;


/**
 * Series container for primitive tri-state boolean (true, false, null). Implementation uses
 * the primitive byte for internal representation.
 */
public final class BooleanSeries extends Series {
  public static final byte NULL_VALUE = Byte.MIN_VALUE;
  public static final byte TRUE_VALUE = 1;
  public static final byte FALSE_VALUE = 0;

  public static class BooleanBatchAnd implements Series.BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return BooleanSeries.NULL_VALUE;
      for(byte b : values) {
        if(BooleanSeries.isFalse(b))
          return BooleanSeries.FALSE_VALUE;
      }
      return BooleanSeries.TRUE_VALUE;
    }
  }

  public static class BooleanBatchOr implements Series.BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return BooleanSeries.NULL_VALUE;
      for(byte b : values) {
        if(BooleanSeries.isTrue(b))
          return BooleanSeries.TRUE_VALUE;
      }
      return BooleanSeries.FALSE_VALUE;
    }
  }

  public static class BooleanBatchLast implements Series.BooleanFunctionEx {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return BooleanSeries.NULL_VALUE;
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

  // CAUTION: The array is final, but values are inherently modifiable
  final byte[] values;

  private BooleanSeries(byte... values) {
    this.values = values;
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
    if(BooleanSeries.isNull(value))
      return DoubleSeries.NULL_VALUE;
    return (double) value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(byte value) {
    if(BooleanSeries.isNull(value))
      return LongSeries.NULL_VALUE;
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
    if(BooleanSeries.isNull(value))
      return StringSeries.NULL_VALUE;
    return isTrue(value) ? "true" : "false";
  }

  @Override
  public boolean isNull(int index) {
    return isNull(this.values[index]);
  }

  @Override
  public BooleanSeries copy() {
    return buildFrom(Arrays.copyOf(this.values, this.values.length));
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

  /**
   * Returns the value of the first element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return first element in the series
   */
  public byte first() {
    assertNotEmpty(this.values);
    return this.values[0];
  }

  /**
   * Returns the value of the last element in the series
   *
   * @throws IllegalStateException if the series is empty
   * @return last element in the series
   */
  public byte last() {
    assertNotEmpty(this.values);
    return this.values[this.values.length-1];
  }

  @Override
  public BooleanSeries slice(int from, int to) {
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
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
  public BooleanSeries sorted() {
    return (BooleanSeries)super.sorted();
  }

  public boolean allTrue() {
    assertNotEmpty(this.values);
    boolean result = true;
    boolean hasValue = false;
    for(byte b : this.values) {
      result &= isTrue(b);
      hasValue |= !isNull(b);
    }
    if(!hasValue)
      throw new IllegalStateException("requires at least one non-null value");
    return result;
  }

  public boolean hasTrue() {
    boolean result = false;
    for(byte b : this.values) {
      result |= isTrue(b);
    }
    return result;
  }

  public boolean allFalse() {
    assertNotEmpty(this.values);
    boolean result = true;
    boolean hasValue = false;
    for(byte b : this.values) {
      result &= isFalse(b);
      hasValue |= !isNull(b);
    }
    if(!hasValue)
      throw new IllegalStateException("requires at least one non-null value");
    return result;
  }

  public boolean hasFalse() {
    boolean result = false;
    for(byte b : this.values) {
      result |= isFalse(b);
    }
    return result;
  }

  public BooleanSeries not() {
    byte[] values = new byte[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      byte value = this.values[i];
      if(isNull(value)) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = isTrue(value) ? FALSE_VALUE : TRUE_VALUE;
      }
    }
    return buildFrom(values);
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
      b.addValues(NULL_VALUE);
    if(hasFalse)
      b.addValues(FALSE_VALUE);
    if(hasTrue)
      b.addValues(TRUE_VALUE);

    return b.build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BooleanSeries{");
    for(byte b : this.values) {
      if(isNull(b)) {
        builder.append("null");
      } else {
        builder.append(isTrue(b) ? "true" : "false");
      }
      builder.append(" ");
    }
    builder.append("}");
    return builder.toString();
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
      Arrays.fill(values, 0, Math.min(offset, values.length), NULL_VALUE);
      System.arraycopy(this.values, 0, values, Math.min(offset, values.length), Math.max(values.length - offset, 0));
    } else {
      System.arraycopy(this.values, Math.min(-offset, values.length), values, 0, Math.max(values.length + offset, 0));
      Arrays.fill(values, Math.max(values.length + offset, 0), values.length, NULL_VALUE);
    }
    return buildFrom(values);
  }

  @Override
  BooleanSeries project(int[] fromIndex) {
    byte[] values = new byte[fromIndex.length];
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] == -1) {
        values[i] = NULL_VALUE;
      } else {
        values[i] = this.values[fromIndex[i]];
      }
    }
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
  public boolean hasNull() {
    for(byte b : this.values)
      if(isNull(b))
        return true;
    return false;
  }

  @Override
  int[] nullIndex() {
    int[] fromIndex = new int[this.values.length];
    int nullCount = 0;

    for(int i=0; i<this.values.length; i++) {
      if(isNull(this.values[i]))
        fromIndex[nullCount++] = i;
    }

    return Arrays.copyOf(fromIndex, nullCount);
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
        return function.apply(input) ? TRUE_VALUE : FALSE_VALUE;
      }
    }, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(BooleanFunctionEx function, Series... series) {
    if(series.length <= 0)
      return empty();

    DataFrame.assertSameLength(series);

    // Note: code-specialization to help hot-spot vm
    if(series.length == 1)
      return map(function, series[0]);
    if(series.length == 2)
      return map(function, series[0], series[1]);
    if(series.length == 3)
      return map(function, series[0], series[1], series[2]);

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
        return NULL_VALUE;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getBoolean(i));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a, Series b) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL_VALUE;
      } else {
        output[i] = function.apply(a.getBoolean(i), b.getBoolean(i));
      }
    }
    return buildFrom(output);
  }

  private static BooleanSeries map(BooleanFunctionEx function, Series a, Series b, Series c) {
    byte[] output = new byte[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL_VALUE;
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
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(final BooleanConditionalEx function, Series... series) {
    return map(new BooleanFunctionEx() {
      @Override
      public byte apply(byte... values) {
        return function.apply(values) ? TRUE_VALUE : FALSE_VALUE;
      }
    }, series);
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunction function, Series series) {
    return builder().addBooleanValues(function.apply(series.getBooleans().valuesBoolean())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanFunctionEx function, Series series) {
    return builder().addValues(function.apply(series.getBooleans().values)).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanConditional function, Series series) {
    return builder().addBooleanValues(function.apply(series.getBooleans().valuesBoolean())).build();
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(BooleanConditionalEx function, Series series) {
    return builder().addBooleanValues(function.apply(series.getBooleans().values)).build();
  }

  public static boolean isNull(byte value) {
    return value == NULL_VALUE;
  }

  public static boolean isFalse(byte value) {
    return value == FALSE_VALUE;
  }

  public static boolean isTrue(byte value) {
    return value != NULL_VALUE && value != FALSE_VALUE;
  }

  public static byte valueOf(boolean value) {
    return value ? TRUE_VALUE : FALSE_VALUE;
  }

  public static byte valueOf(Boolean value) {
    return value == null ? NULL_VALUE : valueOf(value.booleanValue());
  }

  public static byte valueOf(byte value) {
    return isNull(value) ? NULL_VALUE : isFalse(value) ? FALSE_VALUE : TRUE_VALUE;
  }

  public static byte valueOf(Byte value) {
    return value == null ? NULL_VALUE : valueOf((byte)value);
  }

  public static boolean booleanValueOf(byte value) {
    return isTrue(value);
  }

  private static byte[] assertNotEmpty(byte[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  private static byte assertValidValue(byte value) {
    if(value != NULL_VALUE && value != TRUE_VALUE && value != FALSE_VALUE)
      throw new IllegalArgumentException(String.format("Must be either %d, %d, or %d", FALSE_VALUE, TRUE_VALUE, NULL_VALUE));
    return value;
  }

}
