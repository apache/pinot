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
package org.apache.pinot.segment.local.segment.creator.impl.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.local.segment.index.readers.bloom.GuavaBloomFilterReaderUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * On-heap creator for guava bloom filter.
 * <p>TODO: Remove the dependency on {@link BloomFilter} and have our own implementation to prevent guava library
 *          changes that breaks the alignment between creator and reader.
 */
@SuppressWarnings("UnstableApiUsage")
public class OnHeapGuavaBloomFilterCreator implements BloomFilterCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapGuavaBloomFilterCreator.class);

  public static final int TYPE_VALUE = 1;
  public static final int VERSION = 1;

  private final File _bloomFilterFile;
  private final BloomFilter<String> _bloomFilter;
  private final FieldSpec.DataType _dataType;

  // TODO: This method is here for compatibility reasons, should be removed in future PRs
  //  exit_criteria: Not needed in Apache Pinot once #10184 is merged
  @Deprecated
  public OnHeapGuavaBloomFilterCreator(File indexDir, String columnName, int cardinality,
      BloomFilterConfig bloomFilterConfig) {
    this(indexDir, columnName, cardinality, bloomFilterConfig, null);
  }

  public OnHeapGuavaBloomFilterCreator(File indexDir, String columnName, int cardinality,
      BloomFilterConfig bloomFilterConfig, FieldSpec.DataType dataType) {
    _dataType = dataType;
    _bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);
    // Calculate the actual fpp with regards to the max size for the bloom filter
    double fpp = bloomFilterConfig.getFpp();
    int maxSizeInBytes = bloomFilterConfig.getMaxSizeInBytes();
    if (maxSizeInBytes > 0) {
      double minFpp = GuavaBloomFilterReaderUtils.computeFPP(maxSizeInBytes, cardinality);
      fpp = Math.max(fpp, minFpp);
    }
    LOGGER.info("Creating bloom filter with cardinality: {}, fpp: {}", cardinality, fpp);
    _bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, fpp);
  }

  @Override
  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  @Override
  public void add(String value) {
    _bloomFilter.put(value);
  }

  @Override
  public void seal()
      throws IOException {
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      out.writeInt(TYPE_VALUE);
      out.writeInt(VERSION);
      _bloomFilter.writeTo(out);
    }
  }

  @Override
  public void close() {
  }
}
