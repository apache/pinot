package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.lang.ArrayUtils;


/**
 * Series container for primitive tri-state boolean (true, false, null). Implementation uses
 * byte as internal representation.
 */
public final class BooleanSeries extends Series {
  public static final byte NULL_VALUE = Byte.MIN_VALUE;
  public static final byte TRUE_VALUE = 1;
  public static final byte FALSE_VALUE = 0;

  // CAUTION: The array is final, but values are inherently modifiable
  final byte[] values;

  public static class BooleanBatchAnd implements BooleanFunction {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL_VALUE;
      for(byte b : values) {
        if(isFalse(b))
          return FALSE_VALUE;
      }
      return TRUE_VALUE;
    }
  }

  public static class BooleanBatchOr implements BooleanFunction {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL_VALUE;
      for(byte b : values) {
        if(isTrue(b))
          return TRUE_VALUE;
      }
      return FALSE_VALUE;
    }
  }

  public static class BooleanBatchLast implements BooleanFunction {
    @Override
    public byte apply(byte... values) {
      if(values.length <= 0)
        return NULL_VALUE;
      return values[values.length-1];
    }
  }

  public static class Builder {
    final List<Byte> values = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder add(boolean value) {
      return this.add(valueOf(value));
    }

    public Builder add(byte value) {
      this.values.add(assertValidValue(value));
      return this;
    }

    public Builder add(Boolean value) {
      return this.add(valueOf(value));
    }

    public Builder add(Byte value) {
      if(value == null) {
        this.values.add(NULL_VALUE);
      } else {
        this.values.add(assertValidValue(value));
      }
      return this;
    }

    public Builder add(boolean... values) {
      for(boolean v : values)
        this.add(v);
      return this;
    }

    public Builder add(byte... values) {
      for(byte v : values)
        this.add(v);
      return this;
    }

    public Builder add(Boolean... values) {
      for(Boolean v : values)
        this.add(v);
      return this;
    }

    public Builder add(Byte... values) {
      for(Byte v : values)
        this.add(v);
      return this;
    }

    public Builder addBooleans(Collection<Boolean> values) {
      for(Boolean v : values)
        this.add(v);
      return this;
    }

    public Builder add(Collection<Byte> values) {
      for(Byte v : values)
        this.add(v);
      return this;
    }

    public Builder add(BooleanSeries series) {
      for(byte v : series.values)
        this.add(v);
      return this;
    }

    public BooleanSeries build() {
      byte[] values = new byte[this.values.size()];
      int i = 0;
      for(Byte v : this.values) {
        values[i++] = v;
      }
      return new BooleanSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static BooleanSeries buildFrom(byte... values) {
    return new BooleanSeries(values);
  }

  public static BooleanSeries buildFrom(boolean... values) {
    return builder().add(values).build();
  }

  public static BooleanSeries buildFrom(Collection<Byte> values) {
    return builder().add(values).build();
  }

  public static BooleanSeries buildFromBooleans(Collection<Boolean> values) {
    return builder().addBooleans(values).build();
  }

  public static BooleanSeries empty() {
    return new BooleanSeries();
  }

  private BooleanSeries(byte... values) {
    this.values = values;
  }

  @Override
  public BooleanSeries copy() {
    return buildFrom(Arrays.copyOf(this.values, this.values.length));
  }

  @Override
  public DoubleSeries getDoubles() {
    double[] values = new double[this.size()];
    for(int i=0; i<values.length; i++) {
      byte value = this.values[i];
      if(isNull(value)) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = isTrue(value) ? 1.0d : 0.0d;
      }
    }
    return DoubleSeries.buildFrom(values);
  }

  @Override
  public LongSeries getLongs() {
    long[] values = new long[this.size()];
    for(int i=0; i<values.length; i++) {
      byte value = this.values[i];
      if(isNull(value)) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        values[i] = isTrue(value) ? 1L : 0L;
      }
    }
    return LongSeries.buildFrom(values);
  }

  @Override
  public BooleanSeries getBooleans() {
    return this;
  }

  @Override
  public StringSeries getStrings() {
    String[] values = new String[this.size()];
    for(int i=0; i<values.length; i++) {
      byte value = this.values[i];
      if(isNull(value)) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = isTrue(value) ? "true" : "false";
      }
    }
    return StringSeries.buildFrom(values);
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
    for(int i=0; i<this.values.length; i++) {
      values[i] = isTrue(this.values[i]);
    }
    return values;
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

  @Override
  public BooleanSeries map(BooleanFunction function) {
    byte[] newValues = new byte[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      newValues[i] = function.apply(this.values[i]);
    }
    return buildFrom(newValues);
  }

  @Override
  public BooleanSeries aggregate(BooleanFunction function) {
    return buildFrom(function.apply(this.values));
  }

  @Override
  public BooleanSeries append(Series series) {
    byte[] values = new byte[this.size() + series.size()];
    System.arraycopy(this.values, 0, values, 0, this.size());
    System.arraycopy(series.getBooleans().values, 0, values, this.size(), series.size());
    return buildFrom(values);
  }

  public boolean allTrue() {
    assertNotEmpty(this.values);
    boolean result = true;
    for(byte b : this.values) {
      result &= isTrue(b);
    }
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
    for(byte b : this.values) {
      result &= isFalse(b);
    }
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
      b.add(NULL_VALUE);
    if(hasFalse)
      b.add(FALSE_VALUE);
    if(hasTrue)
      b.add(TRUE_VALUE);

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
   * Return a copy of the series with all <b>null</b> values replaced by
   * <b>value</b>.
   *
   * @param value replacement value for <b>null</b>
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
    return Byte.compare(this.values[indexThis], ((BooleanSeries)that).values[indexThat]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static boolean isNull(byte value) {
    return value == NULL_VALUE;
  }

  public static boolean isTrue(byte value) {
    return value == TRUE_VALUE;
  }

  public static boolean isFalse(byte value) {
    return value == FALSE_VALUE;
  }

  public static byte valueOf(boolean value) {
    return value ? TRUE_VALUE : FALSE_VALUE;
  }

  public static byte valueOf(Boolean value) {
    return value == null ? NULL_VALUE : valueOf(value.booleanValue());
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
