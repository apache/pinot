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

import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * On-heap reader for guava bloom filter.
 */
public class OnHeapGuavaBloomFilterReader extends BaseGuavaBloomFilterReader {
  private final long[] _data;

  public OnHeapGuavaBloomFilterReader(PinotDataBuffer dataBuffer) {
    super(dataBuffer);

    int numLongs = (int) (_numBits / Long.SIZE);
    _data = new long[numLongs];
    for (int i = 0; i < numLongs; i++) {
      _data[i] = _valueBuffer.getLong(i * Long.BYTES);
    }
  }

  @Override
  public boolean mightContain(long hash1, long hash2) {
    long combinedHash = hash1;
    for (int i = 0; i < _numHashFunctions; i++) {
      long bitIndex = (combinedHash & Long.MAX_VALUE) % _numBits;
      // NOTE: Guava bloom filter stores bits in a long array. Inside each long value, the bits are stored in the
      //       reverse order (the first bit is stored as the right most bit of the long).
      if ((_data[(int) (bitIndex >>> 6)] & (1L << bitIndex)) == 0) {
        return false;
      }
      combinedHash += hash2;
    }
    return true;
  }
}
