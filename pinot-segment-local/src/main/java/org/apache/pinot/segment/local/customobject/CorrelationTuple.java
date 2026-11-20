/**
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

package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;


/// Intermediate state used by CorrelationAggregationFunction to calculate Pearson's correlation coefficient.
///
/// Keeps the row count, the mean of each column and the co-moments around those means: m2X = sum((x - meanX)^2),
/// m2Y = sum((y - meanY)^2) and cXY = sum((x - meanX) * (y - meanY)). Rows are added with Welford's update and tuples
/// are merged with Chan et al.'s pairwise update, the way VarianceTuple does it, so the result stays accurate when the
/// values are large relative to their spread (raw sums of squares would cancel catastrophically there).
public class CorrelationTuple implements Comparable<CorrelationTuple> {

  private long _count;
  private double _meanX;
  private double _meanY;
  private double _m2X;
  private double _m2Y;
  private double _cXY;

  public CorrelationTuple() {
  }

  public CorrelationTuple(long count, double meanX, double meanY, double m2X, double m2Y, double cXY) {
    _count = count;
    _meanX = meanX;
    _meanY = meanY;
    _m2X = m2X;
    _m2Y = m2Y;
    _cXY = cXY;
  }

  /// Adds a single (x, y) row.
  public void apply(double x, double y) {
    _count++;
    double deltaX = x - _meanX;
    double deltaY = y - _meanY;
    _meanX += deltaX / _count;
    _meanY += deltaY / _count;
    _m2X += deltaX * (x - _meanX);
    _m2Y += deltaY * (y - _meanY);
    _cXY += deltaX * (y - _meanY);
  }

  /// Merges another tuple into this one.
  public void apply(CorrelationTuple other) {
    if (other._count == 0) {
      return;
    }
    if (_count == 0) {
      _count = other._count;
      _meanX = other._meanX;
      _meanY = other._meanY;
      _m2X = other._m2X;
      _m2Y = other._m2Y;
      _cXY = other._cXY;
      return;
    }
    long count = _count + other._count;
    double deltaX = other._meanX - _meanX;
    double deltaY = other._meanY - _meanY;
    // n_a * n_b / n, computed in double to avoid overflowing the long product
    double weight = (double) _count * other._count / count;
    _meanX += deltaX * other._count / count;
    _meanY += deltaY * other._count / count;
    _m2X += other._m2X + deltaX * deltaX * weight;
    _m2Y += other._m2Y + deltaY * deltaY * weight;
    _cXY += other._cXY + deltaX * deltaY * weight;
    _count = count;
  }

  public long getCount() {
    return _count;
  }

  public double getMeanX() {
    return _meanX;
  }

  public double getMeanY() {
    return _meanY;
  }

  public double getM2X() {
    return _m2X;
  }

  public double getM2Y() {
    return _m2Y;
  }

  public double getCXY() {
    return _cXY;
  }

  /// Population Pearson correlation of the accumulated rows, or NaN when it is undefined (fewer than two rows, or a
  /// column with zero variance).
  public double getCorrelation() {
    if (_count < 2 || _m2X <= 0.0 || _m2Y <= 0.0) {
      return Double.NaN;
    }
    double correlation = _cXY / (Math.sqrt(_m2X) * Math.sqrt(_m2Y));
    // The co-moments are accurate, but the ratio can still land an ulp outside [-1, 1]
    return Math.max(-1.0, Math.min(1.0, correlation));
  }

  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + 5 * Double.BYTES);
    byteBuffer.putLong(_count);
    byteBuffer.putDouble(_meanX);
    byteBuffer.putDouble(_meanY);
    byteBuffer.putDouble(_m2X);
    byteBuffer.putDouble(_m2Y);
    byteBuffer.putDouble(_cXY);
    return byteBuffer.array();
  }

  public static CorrelationTuple fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static CorrelationTuple fromByteBuffer(ByteBuffer byteBuffer) {
    return new CorrelationTuple(byteBuffer.getLong(), byteBuffer.getDouble(), byteBuffer.getDouble(),
        byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble());
  }

  /// Orders two tuples by their Pearson correlation, treating an undefined correlation as smaller than any defined one.
  @Override
  public int compareTo(CorrelationTuple correlationTuple) {
    double corr1 = getCorrelation();
    double corr2 = correlationTuple.getCorrelation();
    boolean defined1 = !Double.isNaN(corr1);
    boolean defined2 = !Double.isNaN(corr2);
    if (!defined1) {
      return defined2 ? -1 : 0;
    }
    if (!defined2) {
      return 1;
    }
    return Double.compare(corr1, corr2);
  }
}
