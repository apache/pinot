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


public class MinMaxRangePair implements Comparable<MinMaxRangePair> {
  private double _min;
  private double _max;

  public MinMaxRangePair() {
    this(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
  }

  public MinMaxRangePair(double min, double max) {
    _min = min;
    _max = max;
  }

  public void apply(double value) {
    apply(value, value);
  }

  public void apply(double min, double max) {
    if (min < _min) {
      _min = min;
    }
    if (max > _max) {
      _max = max;
    }
  }

  public void apply(@Nonnull MinMaxRangePair minMaxRangePair) {
    if (minMaxRangePair._min < _min) {
      _min = minMaxRangePair._min;
    }
    if (minMaxRangePair._max > _max) {
      _max = minMaxRangePair._max;
    }
  }

  public double getMin() {
    return _min;
  }

  public double getMax() {
    return _max;
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Double.BYTES);
    byteBuffer.putDouble(_min);
    byteBuffer.putDouble(_max);
    return byteBuffer.array();
  }

  @Nonnull
  public static MinMaxRangePair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static MinMaxRangePair fromByteBuffer(ByteBuffer byteBuffer) {
    return new MinMaxRangePair(byteBuffer.getDouble(), byteBuffer.getDouble());
  }

  @Override
  public int compareTo(@Nonnull MinMaxRangePair minMaxRangePair) {
    double minMaxRange1 = _max - _min;
    double minMaxRange2 = minMaxRangePair._max - minMaxRangePair._min;
    if (minMaxRange1 > minMaxRange2) {
      return 1;
    }
    if (minMaxRange1 < minMaxRange2) {
      return -1;
    }
    return 0;
  }
}
