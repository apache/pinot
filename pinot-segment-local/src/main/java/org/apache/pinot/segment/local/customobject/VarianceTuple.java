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


public class VarianceTuple implements Comparable<VarianceTuple> {
  private long _count;
  private double _sum;
  private double _m2;

  public VarianceTuple(long count, double sum, double m2) {
    _count = count;
    _sum = sum;
    _m2 = m2;
  }

  public void apply(long count, double sum, double m2) {
    if (count == 0) {
      return;
    }
    double currAvg = (_count == 0) ? 0 : _sum / _count;
    double delta = (sum / count) - currAvg;
    _m2 += m2 + delta * delta * count * _count / (count + _count);
    _count += count;
    _sum += sum;
  }

  public void apply(VarianceTuple varianceTuple) {
    if (varianceTuple._count == 0) {
      return;
    }
    double currAvg = (_count == 0) ? 0 : _sum / _count;
    double delta = (varianceTuple._sum / varianceTuple._count) - currAvg;
    _m2 += varianceTuple._m2 + delta * delta * varianceTuple._count * _count / (varianceTuple._count + _count);
    _count += varianceTuple._count;
    _sum += varianceTuple._sum;
  }

  public long getCount() {
    return _count;
  }

  public double getSum() {
    return _sum;
  }

  public double getM2() {
    return _m2;
  }

  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES * 2 + Long.BYTES);
    byteBuffer.putLong(_count);
    byteBuffer.putDouble(_sum);
    byteBuffer.putDouble(_m2);
    return byteBuffer.array();
  }

  public static VarianceTuple fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static VarianceTuple fromByteBuffer(ByteBuffer byteBuffer) {
    return new VarianceTuple(byteBuffer.getLong(), byteBuffer.getDouble(), byteBuffer.getDouble());
  }

  @Override
  public int compareTo(VarianceTuple varianceTuple) {
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
        if (_m2 > varianceTuple._m2) {
          return 1;
        }
        if (_m2 < varianceTuple._m2) {
          return -1;
        }
        return 0;
      }
    }
  }
}
