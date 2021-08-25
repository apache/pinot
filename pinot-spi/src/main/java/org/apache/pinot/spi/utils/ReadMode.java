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
package org.apache.pinot.spi.utils;

/**
 * Enum class for segment read mode:
 * <ul>
 *   <li> heap: Segments are loaded on direct-memory. Note, 'heap' here is a legacy misnomer, and it does not
 *        imply JVM heap. This mode should only be used when we want faster performance than memory-mapped files,
 *        and are also sure that we will never run into OOM. </li>
 *   <li> mmap: Segments are loaded on memory-mapped file. This is the default mode. </li>
 * </ul>
 */
public enum ReadMode {
  heap, mmap;

  public static final ReadMode DEFAULT_MODE = ReadMode.valueOf(CommonConstants.Server.DEFAULT_READ_MODE);

  public static ReadMode getEnum(String strVal) {
    if (strVal.equalsIgnoreCase("heap")) {
      return heap;
    }
    if (strVal.equalsIgnoreCase("mmap") || strVal.equalsIgnoreCase("memorymapped") || strVal
        .equalsIgnoreCase("memorymap")) {
      return mmap;
    }
    throw new IllegalArgumentException("Unknown String Value: " + strVal);
  }
}
