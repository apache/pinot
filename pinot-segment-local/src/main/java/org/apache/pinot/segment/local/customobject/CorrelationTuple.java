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
/// On top of the running sums CovarianceTuple keeps (sum of x, sum of y, sum of x*y and the row count), this tuple
/// also keeps the sum of x^2 and the sum of y^2, which the correlation needs to recover the spread of each column.
public class CorrelationTuple implements Comparable<CorrelationTuple> {

  private double _sumX;
  private double _sumY;
  private double _sumXY;
  private double _sumX2;
  private double _sumY2;
  private long _count;

  public CorrelationTuple(double sumX, double sumY, double sumXY, double sumX2, double sumY2, long count) {
    _sumX = sumX;
    _sumY = sumY;
    _sumXY = sumXY;
    _sumX2 = sumX2;
    _sumY2 = sumY2;
    _count = count;
  }

  public void apply(double sumX, double sumY, double sumXY, double sumX2, double sumY2, long count) {
    _sumX += sumX;
    _sumY += sumY;
    _sumXY += sumXY;
    _sumX2 += sumX2;
    _sumY2 += sumY2;
    _count += count;
  }

  public void apply(CorrelationTuple correlationTuple) {
    _sumX += correlationTuple._sumX;
    _sumY += correlationTuple._sumY;
    _sumXY += correlationTuple._sumXY;
    _sumX2 += correlationTuple._sumX2;
    _sumY2 += correlationTuple._sumY2;
    _count += correlationTuple._count;
  }

  public double getSumX() {
    return _sumX;
  }

  public double getSumY() {
    return _sumY;
  }

  public double getSumXY() {
    return _sumXY;
  }

  public double getSumX2() {
    return _sumX2;
  }

  public double getSumY2() {
    return _sumY2;
  }

  public long getCount() {
    return _count;
  }

  public byte[] toBytes() {
    ByteBuffer byteBuffer =
        ByteBuffer.allocate(Double.BYTES + Double.BYTES + Double.BYTES + Double.BYTES + Double.BYTES + Long.BYTES);
    byteBuffer.putDouble(_sumX);
    byteBuffer.putDouble(_sumY);
    byteBuffer.putDouble(_sumXY);
    byteBuffer.putDouble(_sumX2);
    byteBuffer.putDouble(_sumY2);
    byteBuffer.putLong(_count);
    return byteBuffer.array();
  }

  public static CorrelationTuple fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static CorrelationTuple fromByteBuffer(ByteBuffer byteBuffer) {
    return new CorrelationTuple(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble(),
        byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getLong());
  }

  /// Orders two tuples by their Pearson correlation, treating an empty tuple or one with zero variance in either
  /// column (an undefined correlation) as smaller than any defined one.
  @Override
  public int compareTo(CorrelationTuple correlationTuple) {
    double corr1 = correlation();
    double corr2 = correlationTuple.correlation();
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

  /// Population Pearson correlation of the accumulated values, or NaN when it is undefined (no rows, or a column with
  /// zero variance).
  private double correlation() {
    if (_count == 0) {
      return Double.NaN;
    }
    double varX = _count * _sumX2 - _sumX * _sumX;
    double varY = _count * _sumY2 - _sumY * _sumY;
    if (varX <= 0.0 || varY <= 0.0) {
      return Double.NaN;
    }
    double cov = _count * _sumXY - _sumX * _sumY;
    return cov / Math.sqrt(varX * varY);
  }
}
