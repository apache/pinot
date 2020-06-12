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
package org.apache.pinot.core.io.util;

import java.io.Closeable;


public interface PinotBitSet extends Closeable {

  /**
   * Decode integers starting at a given index.
   * @param index index
   * @return unpacked integer
   */
  int readInt(long index);

  /**
   * Decode integers for a contiguous range of indexes represented by startIndex
   * and length
   * @param startIndex start docId
   * @param length length
   * @param out out array to store the unpacked integers
   */
  void readInt(long startIndex, int length, int[] out);

  /**
   * Encode integer starting at a given index.
   * @param index index
   * @param value integer to encode
   */
  void writeInt(int index, int value);

  /**
   * Encode integers for a contiguous range of indexes represented by startIndex
   * and length
   * @param startIndex start docId
   * @param length length
   * @param buffer array of integers to encode
   */
  void writeInt(int startIndex, int length, int[] buffer);
}
