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
package org.apache.pinot.segment.local.io.util;

import java.io.Closeable;
import java.math.BigDecimal;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Interface for value readers, which read a value at a given index.
 */
public interface ValueReader extends Closeable {

  int getInt(int index);

  long getLong(int index);

  float getFloat(int index);

  double getDouble(int index);

  default BigDecimal getBigDecimal(int index, int numBytesPerValue) {
    return BigDecimalUtils.deserialize(getBytes(index, numBytesPerValue));
  }

  /**
   * Returns un-padded bytes for string.
   * NOTE: The passed in reusable buffer should have capacity of at least {@code numBytesPerValue}.
   */
  byte[] getUnpaddedBytes(int index, int numBytesPerValue, byte[] buffer);

  /**
   * NOTE: The passed in reusable buffer should have capacity of at least {@code numBytesPerValue}.
   */
  String getUnpaddedString(int index, int numBytesPerValue, byte[] buffer);

  /**
   * NOTE: The passed in reusable buffer should have capacity of at least {@code numBytesPerValue}.
   */
  String getPaddedString(int index, int numBytesPerValue, byte[] buffer);

  /**
   * NOTE: Do not reuse buffer for BYTES because the return value can have variable length.
   */
  byte[] getBytes(int index, int numBytesPerValue);

  /**
   * Returns the comparison result of the UTF-8 decoded values.
   */
  int compareUtf8Bytes(int index, int numBytesPerValue, byte[] bytes);

  /**
   * Returns the comparison result of the bytes values.
   */
  int compareBytes(int index, int numBytesPerValue, byte[] bytes);
}
