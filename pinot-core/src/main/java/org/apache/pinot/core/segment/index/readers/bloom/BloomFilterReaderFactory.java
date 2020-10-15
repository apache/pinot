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
package org.apache.pinot.core.segment.index.readers.bloom;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class BloomFilterReaderFactory {
  private BloomFilterReaderFactory() {
  }

  private static final int TYPE_VALUE_OFFSET = 0;
  private static final int VERSION_OFFSET = 4;
  private static final int HEADER_SIZE = 8;

  public static BloomFilterReader getBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap) {
    int typeValue = dataBuffer.getInt(TYPE_VALUE_OFFSET);
    int version = dataBuffer.getInt(VERSION_OFFSET);
    Preconditions.checkState(
        typeValue == OnHeapGuavaBloomFilterCreator.TYPE_VALUE && version == OnHeapGuavaBloomFilterCreator.VERSION,
        "Unsupported bloom filter type value: %s and version: %s", typeValue, version);
    PinotDataBuffer bloomFilterDataBuffer = dataBuffer.view(HEADER_SIZE, dataBuffer.size());
    return onHeap ? new OnHeapGuavaBloomFilterReader(bloomFilterDataBuffer)
        : new OffHeapGuavaBloomFilterReader(bloomFilterDataBuffer);
  }
}
