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
package org.apache.pinot.plugin.stream.push;

public class BufferedRecord {
  private final byte[] _value;
  private final long _arrivalTimestamp;
  private volatile long _offset;

  public BufferedRecord(byte[] value) {
    _value = value;
    _arrivalTimestamp = System.nanoTime();
  }

  void setOffset(long offset) {
    _offset = offset;
  }

  public byte[] getValue() {
    return _value;
  }

  public long getArrivalTimestampNanos() {
    return _arrivalTimestamp;
  }

  public long getOffset() {
    return _offset;
  }

  public int getValueSize() {
    return _value.length;
  }
}
