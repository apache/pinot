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
package org.apache.pinot.core.segment.index.readers;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.pinot.core.bloom.BloomFilter;
import org.apache.pinot.core.bloom.BloomFilterType;
import org.apache.pinot.core.bloom.SegmentBloomFilterFactory;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Bloom filter reader
 */
public class BloomFilterReader {

  private BloomFilter _bloomFilter;

  public BloomFilterReader(PinotDataBuffer bloomFilterBuffer)
      throws IOException {
    byte[] buffer = new byte[(int) bloomFilterBuffer.size()];
    bloomFilterBuffer.copyTo(0, buffer);

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer))) {
      BloomFilterType bloomFilterType = BloomFilterType.valueOf(in.readInt());
      int version = in.readInt();
      _bloomFilter = SegmentBloomFilterFactory.createSegmentBloomFilter(bloomFilterType);
      if (version != _bloomFilter.getVersion()) {
        throw new IOException(
            "Unexpected bloom filter version (type: " + bloomFilterType.toString() + ", version: " + version);
      }
      _bloomFilter.readFrom(in);
    }
  }

  public boolean mightContain(Object key) {
    return _bloomFilter.mightContain(key.toString());
  }
}
