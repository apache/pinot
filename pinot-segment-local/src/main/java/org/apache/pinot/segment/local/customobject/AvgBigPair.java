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
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

public class AvgBigPair implements Comparable<AvgBigPair> {
    private BigDecimal _sum;
    private long _count;

    public AvgBigPair(BigDecimal sum, long count) {
        _sum = sum;
        _count = count;
    }

    public void apply(BigDecimal sum, long count) {
        _sum = _sum.add(sum);
        _count += count;
    }

    public void apply(@Nonnull AvgBigPair avgPair) {
        _sum = _sum.add(avgPair._sum);
        _count += avgPair._count;
    }

    public BigDecimal getSum() {
        return _sum;
    }

    public long getCount() {
        return _count;
    }

    @Nonnull
    public byte[] toBytes() {
        byte[] sumBytes = _sum.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + sumBytes.length + Long.BYTES);
        byteBuffer.putInt(sumBytes.length);
        byteBuffer.put(sumBytes);
        byteBuffer.putLong(_count);
        return byteBuffer.array();
    }

    @Nonnull
    public static AvgBigPair fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    @Nonnull
    public static AvgBigPair fromByteBuffer(ByteBuffer byteBuffer) {
        int sumLength = byteBuffer.getInt();
        byte[] sumBytes = new byte[sumLength];
        byteBuffer.get(sumBytes);
        BigDecimal sum = new BigDecimal(new String(sumBytes, StandardCharsets.UTF_8));
        long count = byteBuffer.getLong();
        return new AvgBigPair(sum, count);
    }

    @Override
    public int compareTo(@Nonnull AvgBigPair avgPair) {
        if (_count == 0) {
            if (avgPair._count == 0) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (avgPair._count == 0) {
                return 1;
            } else {
                BigDecimal avg1 = _sum.divide(BigDecimal.valueOf(_count), RoundingMode.HALF_UP);
                BigDecimal avg2 = avgPair._sum.divide(BigDecimal.valueOf(avgPair._count), RoundingMode.HALF_UP);
                return avg1.compareTo(avg2);
            }
        }
    }
}
