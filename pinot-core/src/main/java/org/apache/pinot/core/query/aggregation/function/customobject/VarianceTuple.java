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
package org.apache.pinot.core.query.aggregation.function.customobject;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

/**
 * Variance data holder.
 * sum2 = x1^2 + x2^2 + ... + xn^2
 */
public class VarianceTuple implements Comparable<VarianceTuple> {
  private double _sum2;
  private double _sum;
  private long _count;

  public VarianceTuple(double sum2, double sum, long count) {
    _sum2 = sum2;
    _sum = sum;
    _count = count;
  }

  public void apply(double sum2, double sum, long count) {
    _sum2 += sum2;
    _sum += sum;
    _count += count;
  }

  public void apply(@Nonnull VarianceTuple varianceTuple) {
    _sum2 += varianceTuple._sum2;
    _sum += varianceTuple._sum;
    _count += varianceTuple._count;
  }

  public double getSum2() {
    return _sum2;
  }

  public double getSum() {
    return _sum;
  }

  public long getCount() {
    return _count;
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Double.BYTES + Long.BYTES);
    byteBuffer.putDouble(_sum2);
    byteBuffer.putDouble(_sum);
    byteBuffer.putLong(_count);
    return byteBuffer.array();
  }

  @Nonnull
  public static VarianceTuple fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static VarianceTuple fromByteBuffer(ByteBuffer byteBuffer) {
    return new VarianceTuple(
        byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getLong());
  }

  @Override
  public int compareTo(@Nonnull VarianceTuple varianceTuple) {
    if (_count == 0) {
      if (varianceTuple._count == 0) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (varianceTuple._count == 0) {
        return 1;
      } else {
        double variance1 = _sum2 / _count - Math.pow(_sum / _count, 2);
        double variance2 = varianceTuple._sum2 / varianceTuple._count
            - Math.pow(varianceTuple._sum / varianceTuple._count, 2);
        if (variance1 > variance2) {
          return 1;
        }
        if (variance1 < variance2) {
          return -1;
        }
        return 0;
      }
    }
  }
}
