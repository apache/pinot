/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import java.io.IOException;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;

public class BloomFilterReader {
  BloomFilter _bloomFilter;

  public BloomFilterReader(PinotDataBuffer bloomFilterBuffer, DataType dataType) throws IOException {
    byte[] buffer = new byte[(int) bloomFilterBuffer.size()];
    bloomFilterBuffer.copyTo(0, buffer);
    _bloomFilter = BloomFilter.deserialize(buffer);
  }

  public boolean mightContain(Object key) {
    return _bloomFilter.isPresent(key.toString());
  }
}
