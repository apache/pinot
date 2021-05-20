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
import com.google.common.primitives.Longs;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;


/**
 * Base implementation of the reader for guava bloom filter.
 * <p>The format of the data should be aligned with the guava bloom filter.
 */
public abstract class BaseGuavaBloomFilterReader implements BloomFilterReader {
  // Format of the data buffer header:
  //   - Strategy ordinal: 1 byte
  //   - Number of hash functions: 1 byte
  //   - Number of long values: 4 bytes
  private static final int STRATEGY_ORDINAL_OFFSET = 0;
  private static final int NUM_HASH_FUNCTIONS_OFFSET = 1;
  private static final int NUM_LONGS_OFFSET = 2;
  private static final int HEADER_SIZE = 6;

  protected final int _numHashFunctions;
  protected final long _numBits;
  protected final PinotDataBuffer _valueBuffer;

  public BaseGuavaBloomFilterReader(PinotDataBuffer dataBuffer) {
    byte strategyOrdinal = dataBuffer.getByte(STRATEGY_ORDINAL_OFFSET);
    Preconditions.checkState(strategyOrdinal == 1, "Unsupported strategy ordinal: %s", strategyOrdinal);
    _numHashFunctions = dataBuffer.getByte(NUM_HASH_FUNCTIONS_OFFSET) & 0xFF;
    _numBits = (long) dataBuffer.getInt(NUM_LONGS_OFFSET) * Long.SIZE;
    _valueBuffer = dataBuffer.view(HEADER_SIZE, dataBuffer.size());
  }

  @Override
  public boolean mightContain(String value) {
    byte[] hash = GuavaBloomFilterReaderUtils.hash(value);
    long hash1 = Longs.fromBytes(hash[7], hash[6], hash[5], hash[4], hash[3], hash[2], hash[1], hash[0]);
    long hash2 = Longs.fromBytes(hash[15], hash[14], hash[13], hash[12], hash[11], hash[10], hash[9], hash[8]);
    return mightContain(hash1, hash2);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
