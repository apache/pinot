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
package org.apache.pinot.segment.local.segment.index.readers.bloom;

import com.google.common.base.Preconditions;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class BloomFilterReaderFactory {
  private BloomFilterReaderFactory() {
  }

  private static final int TYPE_VALUE_OFFSET = 0;
  private static final int VERSION_OFFSET = 4;
  /** Byte offset at which Guava bloom filter bytes start in the legacy v1 format. */
  private static final int GUAVA_PAYLOAD_OFFSET_V1 = 8;
  /**
   * Byte offset at which Guava bloom filter bytes start in the v2 format.
   * V2 header: [TYPE_VALUE (int, 4)][VERSION_V2 (int, 4)][FPP (double, 8)] = 16 bytes.
   */
  private static final int GUAVA_PAYLOAD_OFFSET_V2 = 16;

  public static BloomFilterReader getBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap) {
    int typeValue = dataBuffer.getInt(TYPE_VALUE_OFFSET);
    int version = dataBuffer.getInt(VERSION_OFFSET);
    if (typeValue == OnHeapGuavaBloomFilterCreator.TYPE_VALUE
        && version == OnHeapGuavaBloomFilterCreator.VERSION_V2) {
      // V2 format: [TYPE_VALUE][VERSION_V2][FPP (double)][Guava bytes]
      PinotDataBuffer bloomFilterDataBuffer = dataBuffer.view(GUAVA_PAYLOAD_OFFSET_V2, dataBuffer.size());
      return onHeap ? new OnHeapGuavaBloomFilterReader(bloomFilterDataBuffer)
          : new OffHeapGuavaBloomFilterReader(bloomFilterDataBuffer);
    }
    // Legacy v1 format: [TYPE_VALUE=1][VERSION][Guava bytes]
    Preconditions.checkState(
        typeValue == OnHeapGuavaBloomFilterCreator.TYPE_VALUE && version == OnHeapGuavaBloomFilterCreator.VERSION,
        "Unsupported bloom filter type value: %s and version: %s", typeValue, version);
    PinotDataBuffer bloomFilterDataBuffer = dataBuffer.view(GUAVA_PAYLOAD_OFFSET_V1, dataBuffer.size());
    return onHeap ? new OnHeapGuavaBloomFilterReader(bloomFilterDataBuffer)
        : new OffHeapGuavaBloomFilterReader(bloomFilterDataBuffer);
  }
}
