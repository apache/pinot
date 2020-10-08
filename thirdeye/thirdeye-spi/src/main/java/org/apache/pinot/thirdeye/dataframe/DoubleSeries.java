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
 *
 */

package org.apache.pinot.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.math.stat.correlation.Covariance;
import org.apache.commons.math.stat.correlation.PearsonsCorrelation;


/**
 * Series container for primitive double.
 */
public final class DoubleSeries extends TypedSeries<DoubleSeries> {
  public static final double NULL = Double.NaN;
  public static final double INFINITY = Double.POSITIVE_INFINITY;
  public static final double POSITIVE_INFINITY = Double.POSITIVE_INFINITY;
  public static final double NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;
  public static final double DEFAULT = 0.0d;
  public static final double MIN_VALUE = Double.MIN_VALUE;
  public static final double MAX_VALUE = Double.MAX_VALUE;

  public static final DoubleFunction SUM = new DoubleSum();
  public static final DoubleFunction PRODUCT = new DoubleProduct();
  public static final DoubleFunction FIRST = new DoubleFirst();
  public static final DoubleFunction LAST = new DoubleLast();
  public static final DoubleFunction MIN = new DoubleMin();
  public static final DoubleFunction MAX = new DoubleMax();
  public static final DoubleFunction MEAN = new DoubleMean();
  public static final DoubleFunction MEDIAN = new DoubleMedian();
  public static final DoubleFunction STD = new DoubleStandardDeviation();
  public static final DoubleFunction NEGATIVE = new DoubleNegative();

  public static final class DoubleSum implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;
      // TODO sort, add low to high for accuracy?
      double result = 0.0d;
      for(double v : values)
        result += v;
      return result;
    }
  }

  public static final class DoubleProduct implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;
      // TODO sort for accuracy?
      double result = 1.0d;
      for(double v : values)
        result *= v;
      return result;
    }
  }

  public static final class DoubleMean implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;

      // TODO sort, add low to high for accuracy?
      double sum = 0.0d;
      int count = 0;
      for(double v : values) {
        sum += v;
        count++;
      }
      return sum / count;
    }
  }

  public static final class DoubleMedian implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;

      values = Arrays.copyOf(values, values.length);
      Arrays.sort(values);

      // odd N, return mid
      if(values.length % 2 == 1)
        return values[values.length / 2];

      // even N, return mean of mid
      return (values[values.length / 2 - 1] + values[values.length / 2]) / 2;
    }
  }

  public static final class DoubleFirst implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;
      return values[0];
    }
  }

  public static final class DoubleLast implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;
      return values[values.length - 1];
    }
  }

  public static final class DoubleMin implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if(values.length <= 0)
        return NULL;
      double min = values[0];
      for(double v : values)
        min = Math.min(min, v);
      return min;
    }
  }

  public static final class DoubleMax implements DoubleFunction {
    @Override
    public double apply(double[] values) {
      if (values.length <= 0)
        return NULL;
      double max = values[0];
      for (double v : values)
        max = Math.max(max, v);
      return max;
    }
  }

  public static final class DoubleNegative implements DoubleFunction {
    @Override
    public double apply(double... values) {
      if(values.length <= 0)
        return NULL;
      return -values[0];
    }
  }

  public static final class DoubleStandardDeviation implements DoubleFunction {
    @Override
    public double apply(double... values) {
      if(values.length <= 1)
        return NULL;
      double mean = MEAN.apply(values);
      double var = 0.0;
      for(double v : values)
        var += (v - mean) * (v - mean);
      return Math.sqrt(var / (values.length - 1));
    }
  }

  public static final class DoubleMapZScore implements DoubleFunction {
    final double mean;
    final double std;

    public DoubleMapZScore(double mean, double std) {
      if(std <= 0.0d)
        throw new IllegalArgumentException("std must be greater than 0");
      this.mean = mean;
      this.std = std;
    }

    @Override
    public double apply(double... values) {
      return (values[0] - this.mean) / this.std;
    }
  }

  public static final class DoubleMapNormalize implements DoubleFunction {
    final double min;
    final double max;

    public DoubleMapNormalize(double min, double max) {
      if(min == max)
        throw new IllegalArgumentException("min and max must be different");
      this.min = min;
      this.max = max;
    }

    @Override
    public double apply(double... values) {
      return (values[0] - this.min) / (this.max - this.min);
    }
  }

  public static final class DoubleQuantile implements DoubleFunction {
    final double q;

    public DoubleQuantile(double q) {
      if (q < 0 || q > 1.0)
        throw new IllegalArgumentException(String.format("q must be between 0.0 and 1.0, but was %f", q));
      this.q = q;
    }

    @Override
    public double apply(double... values) {
      if (values.length <= 0)
        return NULL;
      values = Arrays.copyOf(values, values.length);
      Arrays.sort(values);
      double index = (values.length - 1) * this.q;
      int lo = (int) Math.floor(index);
      int hi = (int) Math.ceil(index);
      double diff = values[hi] - values[lo];
      return values[lo] + diff * (index - lo);
    }
  }

  public static class Builder extends Series.Builder {
    final List<double[]> arrays = new ArrayList<>();

    private Builder() {
      // left blank
    }

    public Builder addValues(double... values) {
      this.arrays.add(values);
      return this;
    }

    public Builder addValues(double value) {
      return this.addValues(new double[] { value });
    }

    public Builder addValues(Collection<Double> values) {
      double[] newValues = new double[values.size()];
      int i = 0;
      for(Double v : values)
        newValues[i++] = valueOf(v);
      return this.addValues(newValues);
    }

    public Builder addValues(Double... values) {
      return this.addValues(Arrays.asList(values));
    }

    public Builder addValues(Double value) {
      return this.addValues(new double[] { valueOf(value) });
    }

    public Builder fillValues(int count, double value) {
      double[] values = new double[count];
      Arrays.fill(values, value);
      return this.addValues(values);
    }

    public Builder fillValues(int count, Double value) {
      return this.fillValues(count, valueOf(value));
    }

    @Override
    public Builder addSeries(Collection<Series> series) {
      for(Series s : series)
        this.addValues(s.getDoubles().values);
      return this;
    }

    @Override
    public DoubleSeries build() {
      int totalSize = 0;
      for(double[] array : this.arrays)
        totalSize += array.length;

      int offset = 0;
      double[] values = new double[totalSize];
      for(double[] array : this.arrays) {
        System.arraycopy(array, 0, values, offset, array.length);
        offset += array.length;
      }

      return new DoubleSeries(values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DoubleSeries buildFrom(double... values) {
    return new DoubleSeries(values);
  }

  public static DoubleSeries empty() {
    return new DoubleSeries();
  }

  public static DoubleSeries nulls(int size) {
    return builder().fillValues(size, NULL).build();
  }

  public static DoubleSeries zeros(int size) {
    return builder().fillValues(size, 0.0d).build();
  }

  public static DoubleSeries ones(int size) {
    return builder().fillValues(size, 1.0d).build();
  }

  public static DoubleSeries fillValues(int size, double value) { return builder().fillValues(size, value).build(); }

  // CAUTION: The array is final, but values are inherently modifiable
  private final double[] values;

  private DoubleSeries(double... values) {
    this.values = values;
  }

  @Override
  public Builder getBuilder() {
    return new Builder();
  }

  @Override
  public DoubleSeries getDoubles() {
    return this;
  }

  @Override
  public double getDouble(int index) {
    return getDouble(this.values[index]);
  }

  public static double getDouble(double value) {
    return value;
  }

  @Override
  public long getLong(int index) {
    return getLong(this.values[index]);
  }

  public static long getLong(double value) {
    if(isNull(value))
      return LongSeries.NULL;
    if(value == NEGATIVE_INFINITY)
      return LongSeries.MIN_VALUE;
    return (long) value;
  }

  @Override
  public byte getBoolean(int index) {
    return getBoolean(this.values[index]);
  }

  public static byte getBoolean(double value) {
    if(isNull(value))
      return BooleanSeries.NULL;
    return BooleanSeries.valueOf(value != 0.0d);
  }

  @Override
  public String getString(int index) {
    return getString(this.values[index]);
  }

  public static String getString(double value) {
    if(isNull(value))
      return StringSeries.NULL;
    return String.valueOf(value);
  }

  @Override
  public Object getObject(int index) {
    return getObject(this.values[index]);
  }

  public static Object getObject(double value) {
    if(isNull(value))
      return ObjectSeries.NULL;
    return value;
  }

  public double get(int index) {
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
    return SeriesType.DOUBLE;
  }

  public double[] values() {
    return this.values;
  }

  public double value() {
    if(this.size() != 1)
      throw new IllegalStateException("Series must contain exactly one element");
    return this.values[0];
  }

  /**
   * Returns the contents of the series wrapped as list.
   *
   * @return list of series elements
   */
  public List<Double> toList() {
    Double[] values = new Double[this.values.length];
    for(int i=0; i<this.values.length; i++) {
      if(!this.isNull(i))
        values[i] = this.values[i];
    }
    return Arrays.asList(values);
  }

  @Override
  public DoubleSeries slice(int from, int to) {
    from = Math.max(Math.min(this.size(), from), 0);
    to = Math.max(Math.min(this.size(), to), 0);
    return buildFrom(Arrays.copyOfRange(this.values, from, to));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DoubleSeries{");
    for(double d : this.values) {
      if(isNull(d)) {
        builder.append("null");
      } else {
        builder.append(d);
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

  @Override
  public DoubleSeries sum() {
    return this.aggregate(SUM);
  }

  @Override
  public DoubleSeries product() {
    return this.aggregate(PRODUCT);
  }

  @Override
  public DoubleSeries min() {
    return this.aggregate(MIN);
  }

  @Override
  public DoubleSeries max() {
    return this.aggregate(MAX);
  }

  @Override
  public DoubleSeries mean() {
    return this.aggregate(MEAN);
  }

  @Override
  public DoubleSeries median() {
    return this.aggregate(MEDIAN);
  }

  @Override
  public DoubleSeries std() {
    return this.aggregate(STD);
  }

  /**
   * Returns the quantile from the series as indicated by {@code q}. Applies linear interpolation
   * between two values if the exact quantile does not align with the index of series values.
   *
   * @param q quantile rank between {@code [0.0, 1.0]} (bounds inclusive)
   *
   * @return series with quantile value or {@code NULL} if invalid
   * @throws IllegalArgumentException if the quantile rank {@code q} is not between {@code [0.0, 1.0]}.
   */
  public DoubleSeries quantile(double q) {
    return this.aggregate(new DoubleQuantile(q));
  }

  public double corr(Series other) {
    return corr(this, other);
  }

  public double cov(Series other) {
    return cov(this, other);
  }

  /**
   * Returns a copy of the series with values normalized to within a range of {@code [0.0, 1.0]}.
   *
   * @return series copy with range normalized values
   * @throws IllegalArgumentException if series min equals max
   */
  public DoubleSeries normalize() {
    return this.map(new DoubleMapNormalize(this.min().value(), this.max().value()));
  }

  /**
   * Returns a copy of the series with values normalized to sum to {@code 1.0}.
   *
   * @return series copy with summation normalized values
   * @throws ArithmeticException if series sums to zero
   */
  public DoubleSeries normalizeSum() {
    return this.divide(this.sum());
  }

  /**
   * Returns a copy of the series with values centered on their mean and range normalized via
   * standard deviation.
   *
   * @return series copy with zscore normalized values
   * @throws IllegalArgumentException if series standard deviation is zero
   */
  public DoubleSeries zscore() {
    return this.map(new DoubleMapZScore(this.mean().value(), this.std().value()));
  }

  /**
   * Returns a copy of the series with values rounded towards the nearest {@code nDecimals}-th decimal.
   * <br/><b>NOTE:</b> a negative number for {@code nDecimals} will round to powers of 10.
   *
   * @param nDecimals number of decimals to round to
   * @return series copy with values rounded towards the nearest given decimal
   */
  public DoubleSeries round(int nDecimals) {
    final double multiplier = Math.pow(10, nDecimals);
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return Math.round(values[0] * multiplier) / multiplier;
      }
    });
  }

  /**
   * Returns a copy of the series with absolute values ({@code "|x|"}).
   *
   * @return series copy with absolute values
   */
  public DoubleSeries abs() {
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return Math.abs(values[0]);
      }
    });
  }

  /**
   * Returns a copy of the series with the natural log of values.
   *
   * @return series copy with log values
   */
  public DoubleSeries log() {
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return Math.log(values[0]);
      }
    });
  }

  public DoubleSeries add(Series other) {
    if(other.size() == 1)
      return this.add(other.getDouble(0));
    return map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] + values[1];
      }
    }, this, other);
  }

  public DoubleSeries add(final double constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] + constant;
      }
    });
  }

  public DoubleSeries subtract(Series other) {
    if(other.size() == 1)
      return this.subtract(other.getDouble(0));
    return map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] - values[1];
      }
    }, this, other);
  }

  public DoubleSeries subtract(final double constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] - constant;
      }
    });
  }

  public DoubleSeries multiply(Series other) {
    if(other.size() == 1)
      return this.multiply(other.getDouble(0));
    return map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] * values[1];
      }
    }, this, other);
  }

  public DoubleSeries multiply(final double constant) {
    if(isNull(constant))
      return nulls(this.size());
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] * constant;
      }
    });
  }

  public DoubleSeries divide(Series other) {
    if(other.size() == 1)
      return this.divide(other.getDouble(0));
    DoubleSeries o = other.getDoubles();
    if(o.contains(0.0d))
      throw new ArithmeticException("/ by zero");
    return map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] / values[1];
      }
    }, this, o);
  }

  public DoubleSeries divide(final double constant) {
    if(isNull(constant))
      return nulls(this.size());
    if(constant == 0.0d)
      throw new ArithmeticException("/ by zero");
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] / constant;
      }
    });
  }

  public DoubleSeries pow(Series other) {
    if(other.size() == 1)
      return this.pow(other.getDouble(0));
    return map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return Math.pow(values[0], values[1]);
      }
    }, this, other);
  }

  public DoubleSeries pow(final double constant) {
    return this.map(new DoubleFunction() {
      @Override
      public double apply(double... values) {
        return Math.pow(values[0], constant);
      }
    });
  }

  public BooleanSeries eq(Series other) {
    if(other.size() == 1)
      return this.eq(other.getDouble(0));
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] == values[1];
      }
    }, this, other);
  }

  public BooleanSeries eq(final double constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] == constant;
      }
    });
  }

  public BooleanSeries eq(final double constant, final double epsilon) {
    return this.eq(fillValues(this.size(), constant), epsilon);
  }

  public BooleanSeries eq(Series other, final double epsilon) {
    if(other.size() == 1)
      return this.eq(other.getDouble(0), epsilon);
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] - epsilon <= values[1] && values[0] + epsilon >= values[1];
      }
    }, this, other);
  }

  public BooleanSeries ne(final double constant) {
    return this.eq(constant).not();
  }

  public BooleanSeries gt(final double constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] > constant;
      }
    });
  }

  public BooleanSeries gt(Series other) {
    if(other.size() == 1)
      return this.gt(other.getLong(0));
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] > values[1];
      }
    }, this, other);
  }

  public BooleanSeries gte(final double constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] >= constant;
      }
    });
  }

  public BooleanSeries gte(Series other) {
    if(other.size() == 1)
      return this.gte(other.getLong(0));
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] >= values[1];
      }
    }, this, other);
  }

  public BooleanSeries lt(final double constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] < constant;
      }
    });
  }

  public BooleanSeries lt(Series other) {
    if(other.size() == 1)
      return this.lt(other.getLong(0));
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] < values[1];
      }
    }, this, other);
  }

  public BooleanSeries lte(final double constant) {
    if(isNull(constant))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] <= constant;
      }
    });
  }

  public BooleanSeries lte(Series other) {
    if(other.size() == 1)
      return this.lte(other.getLong(0));
    return map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] <= values[1];
      }
    }, this, other);
  }

  public BooleanSeries between(final double startIncl, final double endExcl) {
    if(isNull(startIncl) || isNull(endExcl))
      return BooleanSeries.nulls(this.size());
    return this.map(new DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return values[0] >= startIncl && values[0] < endExcl;
      }
    });
  }

  @Override
  public DoubleSeries set(BooleanSeries mask, Series other) {
    if(other.size() == 1)
      return this.set(mask, other.getDouble(0));
    assertSameLength(this, mask ,other);

    double[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<this.values.length; i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i)))
        values[i] = other.getDouble(i);
    }
    return buildFrom(values);
  }

  public DoubleSeries set(BooleanSeries mask, double value) {
    assertSameLength(this, mask);
    double[] values = new double[this.values.length];
    for(int i=0; i<mask.size(); i++) {
      if(BooleanSeries.isTrue(mask.getBoolean(i))) {
        values[i] = value;
      } else {
        values[i] = this.values[i];
      }
    }
    return buildFrom(values);
  }

  public int count(double value) {
    int count = 0;
    for(double v : this.values)
      if(nullSafeDoubleComparator(v, value) == 0)
        count++;
    return count;
  }

  public boolean contains(double value) {
    return this.count(value) > 0;
  }

  public DoubleSeries replace(double find, double by) {
    if(isNull(find))
      return this.fillNull(by);
    return this.set(this.eq(find), by);
  }

  public int find(double value, double epsilon) {
    return this.find(value, epsilon, 0);
  }

  public int find(double value, double epsilon, int startOffset) {
    for(int i=startOffset; i<this.values.length; i++)
      if((this.values[i] >= value - epsilon
          && this.values[i] <= value + epsilon)
          || isNull(this.values[i]) && isNull(value))
        return i;
    return -1;
  }

  @Override
  public DoubleSeries filter(BooleanSeries filter) {
    return this.set(filter.fillNull().not(), NULL);
  }

  @Override
  public DoubleSeries fillNull() {
    return this.fillNull(DEFAULT);
  }

  /**
   * Return a copy of the series with all <b>null</b> values replaced by
   * {@code value}.
   *
   * @param value replacement value for <b>null</b>
   * @return series copy without nulls
   */
  public DoubleSeries fillNull(double value) {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(isNull(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  public DoubleSeries fillInfinite(double value) {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    for(int i=0; i<values.length; i++) {
      if(Double.isInfinite(values[i])) {
        values[i] = value;
      }
    }
    return buildFrom(values);
  }

  @Override
  DoubleSeries project(int[] fromIndex) {
    double[] values = new double[fromIndex.length];
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

    DoubleSeries that = (DoubleSeries) o;

    return Arrays.equals(this.values, that.values);
  }

  @Override
  int compare(Series that, int indexThis, int indexThat) {
    return nullSafeDoubleComparator(this.values[indexThis], that.getDouble(indexThat));
  }

  @Override
  int hashCode(int index) {
    return (int) Double.doubleToRawLongBits(this.values[index]);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static DoubleSeries map(DoubleFunction function, Series... series) {
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

    double[] input = new double[series.length];
    double[] output = new double[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return buildFrom(output);
  }

  private static double mapRow(DoubleFunction function, Series[] series, double[] input, int row) {
    for(int j=0; j<series.length; j++) {
      double value = series[j].getDouble(row);
      if(isNull(value))
        return NULL;
      input[j] = value;
    }
    return function.apply(input);
  }

  private static DoubleSeries mapUnrolled(DoubleFunction function, Series a) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  private static DoubleSeries mapUnrolled(DoubleFunction function, Series a, Series b) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getDouble(i), b.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  private static DoubleSeries mapUnrolled(DoubleFunction function, Series a, Series b, Series c) {
    double[] output = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      if(a.isNull(i) || b.isNull(i) || c.isNull(i)) {
        output[i] = NULL;
      } else {
        output[i] = function.apply(a.getDouble(i), b.getDouble(i), c.getDouble(i));
      }
    }
    return buildFrom(output);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(DoubleConditional function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    assertSameLength(series);

    double[] input = new double[series.length];
    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      output[i] = mapRow(function, series, input, i);
    }

    return BooleanSeries.buildFrom(output);
  }

  private static byte mapRow(DoubleConditional function, Series[] series, double[] input, int row) {
    for(int j=0; j<series.length; j++) {
      double value = series[j].getDouble(row);
      if(isNull(value))
        return BooleanSeries.NULL;
      input[j] = value;
    }
    return BooleanSeries.valueOf(function.apply(input));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static DoubleSeries aggregate(DoubleFunction function, Series series) {
    return buildFrom(function.apply(series.dropNull().getDoubles().values));
  }

  /**
   * @see Series#aggregate(Function)
   */
  public static BooleanSeries aggregate(DoubleConditional function, Series series) {
    return BooleanSeries.builder().addBooleanValues(function.apply(series.dropNull().getDoubles().values)).build();
  }

  public static double corr(Series a, Series b) {
    if(a.hasNull() || b.hasNull())
      return NULL;
    return new PearsonsCorrelation().correlation(a.getDoubles().values(), b.getDoubles().values());
  }

  public static double cov(Series a, Series b) {
    if(a.hasNull() || b.hasNull())
      return NULL;
    return new Covariance().covariance(a.getDoubles().values(), b.getDoubles().values());
  }

  private static int nullSafeDoubleComparator(double a, double b) {
    if(isNull(a) && isNull(b))
      return 0;
    if(isNull(a))
      return -1;
    if(isNull(b))
      return 1;
    return Double.compare(a, b);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.values);
  }

  public static double valueOf(Double value) {
    if(value == null)
      return NULL;
    return value;
  }

  public static boolean isNull(double value) {
    return Double.isNaN(value);
  }

  private static double[] assertNotEmpty(double[] values) {
    if(values.length <= 0)
      throw new IllegalStateException("Must contain at least one value");
    return values;
  }

  @Override
  public DoubleSeries shift(int offset) {
    double[] values = new double[this.values.length];
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
  public DoubleSeries sorted() {
    double[] values = Arrays.copyOf(this.values, this.values.length);
    Arrays.sort(values);

    // order NaNs first
    int count = 0;
    while(count < values.length && isNull(values[values.length - count - 1]))
      count++;

    if(count <= 0 || count >= values.length)
      return buildFrom(values);

    double[] newValues = new double[values.length];
    Arrays.fill(newValues, 0, count, Double.NaN);
    System.arraycopy(values, 0, newValues, count, values.length - count);

    return buildFrom(newValues);
  }

  @Override
  int[] sortedIndex() {
    List<DoubleSortTuple> tuples = new ArrayList<>();
    for (int i = 0; i < this.values.length; i++) {
      tuples.add(new DoubleSortTuple(this.values[i], i));
    }

    Collections.sort(tuples, new Comparator<DoubleSortTuple>() {
      @Override
      public int compare(DoubleSortTuple a, DoubleSortTuple b) {
        return nullSafeDoubleComparator(a.value, b.value);
      }
    });

    int[] fromIndex = new int[tuples.size()];
    for (int i = 0; i < tuples.size(); i++) {
      fromIndex[i] = tuples.get(i).index;
    }
    return fromIndex;
  }

  static final class DoubleSortTuple {
    final double value;
    final int index;

    DoubleSortTuple(double value, int index) {
      this.value = value;
      this.index = index;
    }
  }
}
