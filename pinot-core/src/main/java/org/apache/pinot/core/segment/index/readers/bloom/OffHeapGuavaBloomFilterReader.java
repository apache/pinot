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
import com.google.common.primitives.Longs;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Off-heap reader for guava bloom filter.
 * <p>The behavior should be aligned with {@link com.google.common.hash.BloomFilter}.
 */
@SuppressWarnings("UnstableApiUsage")
public class OffHeapGuavaBloomFilterReader implements BloomFilterReader {
  // Format of the data buffer header:
  //   - Strategy ordinal: 1 byte
  //   - Number of hash functions: 1 byte
  //   - Number of long values: 4 bytes
  private static final int STRATEGY_ORDINAL_OFFSET = 0;
  private static final int NUM_HASH_FUNCTIONS_OFFSET = 1;
  private static final int NUM_LONGS_OFFSET = 2;
  private static final int HEADER_SIZE = 6;

  private final int _numHashFunctions;
  private final long _numBits;
  private final PinotDataBuffer _valueBuffer;

  public OffHeapGuavaBloomFilterReader(PinotDataBuffer dataBuffer) {
    byte strategyOrdinal = dataBuffer.getByte(STRATEGY_ORDINAL_OFFSET);
    Preconditions.checkState(strategyOrdinal == 1, "Unsupported strategy ordinal: %s", strategyOrdinal);
    _numHashFunctions = dataBuffer.getByte(NUM_HASH_FUNCTIONS_OFFSET) & 0xFF;
    _numBits = (long) dataBuffer.getInt(NUM_LONGS_OFFSET) * Long.SIZE;
    _valueBuffer = dataBuffer.view(HEADER_SIZE, dataBuffer.size());
  }

  @Override
  public boolean mightContain(String value) {
    return mightContain(GuavaBloomFilterReaderUtils.hash(value));
  }

  @Override
  public boolean mightContain(byte[] hash) {
    long hash1 = Longs.fromBytes(hash[7], hash[6], hash[5], hash[4], hash[3], hash[2], hash[1], hash[0]);
    long hash2 = Longs.fromBytes(hash[15], hash[14], hash[13], hash[12], hash[11], hash[10], hash[9], hash[8]);
    long combinedHash = hash1;
    for (int i = 0; i < _numHashFunctions; i++) {
      long bitIndex = (combinedHash & Long.MAX_VALUE) % _numBits;
      // NOTE: Guava bloom filter stores bits in a long array. Inside each long value, the bits are stored in the
      //       reverse order (the first bit is stored as the right most bit of the long).
      int longIndex = (int) (bitIndex >>> 6);
      int bitIndexInLong = (int) (bitIndex & 0x3F);
      int byteIndex = (longIndex << 3) | (7 - (bitIndexInLong >>> 3));
      if ((_valueBuffer.getByte(byteIndex) & (1 << (bitIndexInLong & 7))) == 0) {
        return false;
      }
      combinedHash += hash2;
    }
    return true;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
