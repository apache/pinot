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
package org.apache.pinot.segment.spi.compression;

public enum ChunkCompressionType {
  PASS_THROUGH(0), SNAPPY(1), ZSTANDARD(2), LZ4(3), LZ4_LENGTH_PREFIXED(4);

  private static final ChunkCompressionType[] VALUES = values();

  private final int _value;

  ChunkCompressionType(int value) {
    _value = value;
  }

  public int getValue() {
    return _value;
  }

  public static ChunkCompressionType valueOf(int ordinal) {
    if (ordinal < 0 || ordinal >= VALUES.length) {
      throw new IllegalArgumentException("invalid ordinal " + ordinal);
    }
    return VALUES[ordinal];
  }
}
