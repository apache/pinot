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

public class LastWithTimePair implements Comparable<LastWithTimePair> {
  private double _data;
  private long _time;

  public LastWithTimePair(double data, long time) {
    _data = data;
    _time = time;
  }

  public void apply(double data, long time) {
    if (time >= _time) {
      _data = data;
      _time = time;
    }
  }

  public void apply(LastWithTimePair lastWithTimePair) {
    if (lastWithTimePair._time >= _time) {
      _data = lastWithTimePair._data;
      _time = lastWithTimePair._time;
    }
  }

  public double getData() {
    return _data;
  }

  public long getTime() {
    return _time;
  }

  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Long.BYTES);
    byteBuffer.putDouble(_data);
    byteBuffer.putLong(_time);
    return byteBuffer.array();
  }

  public static LastWithTimePair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static LastWithTimePair fromByteBuffer(ByteBuffer byteBuffer) {
    return new LastWithTimePair(byteBuffer.getDouble(), byteBuffer.getLong());
  }

  @Override
  public int compareTo(LastWithTimePair lastWithTimePair) {
    if (lastWithTimePair._time != _time) {
      if (_time < lastWithTimePair._time) {
        return -1;
      } else {
        return 1;
      }
    } else if (_data < lastWithTimePair._data) {
      return -1;
    } else if (_data == lastWithTimePair._data) {
      return 0;
    } else {
      return 1;
    }
  }
}
