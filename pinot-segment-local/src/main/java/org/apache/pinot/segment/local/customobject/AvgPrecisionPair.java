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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * AvgPrecisionPair stores the sum as BigDecimal and count for high-precision average calculations.
 * This is used by the AVGPRECISION aggregation function to maintain precision when computing averages.
 */
public class AvgPrecisionPair implements Comparable<AvgPrecisionPair> {
  private BigDecimal _sum;
  private long _count;

  public AvgPrecisionPair() {
    this(BigDecimal.ZERO, 0L);
  }

  public AvgPrecisionPair(BigDecimal sum, long count) {
    _sum = sum;
    _count = count;
  }

  public void apply(BigDecimal sum, long count) {
    _sum = _sum.add(sum);
    _count += count;
  }

  public void apply(AvgPrecisionPair avgPrecisionPair) {
    _sum = _sum.add(avgPrecisionPair._sum);
    _count += avgPrecisionPair._count;
  }

  public void apply(BigDecimal value) {
    _sum = _sum.add(value);
    _count++;
  }

  public BigDecimal getSum() {
    return _sum;
  }

  public long getCount() {
    return _count;
  }

  public byte[] toBytes() {
    byte[] sumBytes = BigDecimalUtils.serialize(_sum);
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + sumBytes.length + Long.BYTES);
    byteBuffer.putInt(sumBytes.length);
    byteBuffer.put(sumBytes);
    byteBuffer.putLong(_count);
    return byteBuffer.array();
  }

  public static AvgPrecisionPair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static AvgPrecisionPair fromByteBuffer(ByteBuffer byteBuffer) {
    int sumBytesLength = byteBuffer.getInt();
    byte[] sumBytes = new byte[sumBytesLength];
    byteBuffer.get(sumBytes);
    BigDecimal sum = BigDecimalUtils.deserialize(sumBytes);
    long count = byteBuffer.getLong();
    return new AvgPrecisionPair(sum, count);
  }

  @Override
  public int compareTo(AvgPrecisionPair avgPrecisionPair) {
    if (_count == 0) {
      if (avgPrecisionPair._count == 0) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (avgPrecisionPair._count == 0) {
        return 1;
      } else {
        BigDecimal avg1 = _sum.divide(BigDecimal.valueOf(_count),
                RoundingMode.HALF_EVEN);
        BigDecimal avg2 = avgPrecisionPair._sum.divide(
                BigDecimal.valueOf(avgPrecisionPair._count), RoundingMode.HALF_EVEN);
        return avg1.compareTo(avg2);
      }
    }
  }
}
