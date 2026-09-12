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


/// On-heap creator for guava bloom filter.
///
/// TODO: Remove the dependency on [BloomFilter] and have our own implementation to prevent guava library
///          changes that breaks the alignment between creator and reader.
@SuppressWarnings("UnstableApiUsage")
public class OnHeapGuavaBloomFilterCreator implements BloomFilterCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapGuavaBloomFilterCreator.class);

  public static final int TYPE_VALUE = 1;
  /** V1 format (legacy): {@code [TYPE_VALUE=1 (int)][VERSION=1 (int)][Guava bytes...]}. Not written by new code. */
  public static final int VERSION = 1;
  /**
   * V2 format (current write format):
   * {@code [TYPE_VALUE=1 (int)][VERSION_V2=2 (int)][effective FPP (double, 8 B)][Guava bytes...]}.
   * Stores the effective fpp in the header so {@code BloomFilterHandler} can detect config changes exactly.
   * Old servers (pre-1.6) that only validate {@code VERSION=1} will throw on load; rolling upgrades should
   * complete before forcing a segment rebuild.
   */
  public static final int VERSION_V2 = 2;
  /** Byte offset of the fpp field in a v2 bloom filter file (after TYPE_VALUE + VERSION_V2). */
  public static final int FPP_OFFSET = 8;

  private final File _bloomFilterFile;
  private final BloomFilter<String> _bloomFilter;
  private final FieldSpec.DataType _dataType;
  private final double _effectiveFpp;

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
    _effectiveFpp = fpp;
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
      out.writeInt(VERSION_V2);
      out.writeDouble(_effectiveFpp);
      _bloomFilter.writeTo(out);
    }
  }

  @Override
  public void close() {
  }
}
