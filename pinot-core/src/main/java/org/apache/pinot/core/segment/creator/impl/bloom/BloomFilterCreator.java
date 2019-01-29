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
package org.apache.pinot.core.segment.creator.impl.bloom;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.pinot.core.bloom.BloomFilter;
import org.apache.pinot.core.bloom.BloomFilterUtil;
import org.apache.pinot.core.bloom.SegmentBloomFilterFactory;
import org.apache.pinot.core.segment.creator.impl.V1Constants;


/**
 * Bloom filter creator
 *
 * Note:
 * 1. Currently, we limit the filter size to 1MB to avoid the heap overhead. We can remove it once we have the offheap
 *    implementation of the bloom filter.
 * 2. When capping the bloom filter to 1MB, max false pos steeply grows from 1 million cardinality. If the column has
 *    larger than "5 million" cardinality, it is not recommended to use bloom filter since maxFalsePosProb is already
 *    0.45 when the filter size is 1MB.
 */
public class BloomFilterCreator implements AutoCloseable {
  private static double DEFAULT_MAX_FALSE_POS_PROBABILITY = 0.05;
  private static int MB_IN_BITS = 8388608;

  private BloomFilter _bloomFilter;
  private File _bloomFilterFile;

  public BloomFilterCreator(File indexDir, String columnName, int cardinality) {
    _bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);
    double maxFalsePosProbability = BloomFilterUtil
        .computeMaxFalsePositiveProbabilityForNumBits(cardinality, MB_IN_BITS, DEFAULT_MAX_FALSE_POS_PROBABILITY);
    _bloomFilter = SegmentBloomFilterFactory.createSegmentBloomFilter(cardinality, maxFalsePosProbability);
  }

  @Override
  public void close()
      throws IOException {
    try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      outputStream.writeInt(_bloomFilter.getBloomFilterType().getValue());
      outputStream.writeInt(_bloomFilter.getVersion());
      _bloomFilter.writeTo(outputStream);
    }
  }

  public void add(Object input) {
    _bloomFilter.add(input.toString());
  }
}
