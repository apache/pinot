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
import javax.annotation.Nonnull;


/**
 * Intermediate state used by CovarianceAggregationFunction which helps calculate
 * population covariance and sample covariance
 */
public class CovarianceTuple implements Comparable<CovarianceTuple> {

  private double _sumX;
  private double _sumY;
  private double _sumXY;
  private long _count;

  public CovarianceTuple(double sumX, double sumY, double sumXY, long count) {
    _sumX = sumX;
    _sumY = sumY;
    _sumXY = sumXY;
    _count = count;
  }

  public void apply(double sumX, double sumY, double sumXY, long count) {
    _sumX += sumX;
    _sumY += sumY;
    _sumXY += sumXY;
    _count += count;
  }

  public void apply(@Nonnull CovarianceTuple covarianceTuple) {
    _sumX += covarianceTuple._sumX;
    _sumY += covarianceTuple._sumY;
    _sumXY += covarianceTuple._sumXY;
    _count += covarianceTuple._count;
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

  public long getCount() {
    return _count;
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Double.BYTES + Double.BYTES + Long.BYTES);
    byteBuffer.putDouble(_sumX);
    byteBuffer.putDouble(_sumY);
    byteBuffer.putDouble(_sumXY);
    byteBuffer.putLong(_count);
    return byteBuffer.array();
  }

  @Nonnull
  public static CovarianceTuple fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static CovarianceTuple fromByteBuffer(ByteBuffer byteBuffer) {
    return new CovarianceTuple(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble(),
        byteBuffer.getLong());
  }

  @Override
  public int compareTo(@Nonnull CovarianceTuple covarianceTuple) {
    if (_count == 0) {
      if (covarianceTuple._count == 0) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (covarianceTuple._count == 0) {
        return 1;
      } else {
        double cov1 = _sumXY / _count - (_sumX / _count) * (_sumY / _count);
        double cov2 =
            covarianceTuple._sumXY / covarianceTuple._count - (covarianceTuple._sumX / covarianceTuple._count) * (
                covarianceTuple._sumY / covarianceTuple._count);
        if (cov1 > cov2) {
          return 1;
        } else if (cov1 < cov2) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }
}
