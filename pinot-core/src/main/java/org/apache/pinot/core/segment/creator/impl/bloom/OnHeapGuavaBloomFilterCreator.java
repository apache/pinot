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

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.core.segment.creator.BloomFilterCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.spi.config.table.BloomFilterConfig;


/**
 * On-heap creator for guava bloom filter.
 * <p>TODO: Remove the dependency on {@link BloomFilter} and have our own implementation to prevent guava library
 *          changes that breaks the alignment between creator and reader.
 */
@SuppressWarnings("UnstableApiUsage")
public class OnHeapGuavaBloomFilterCreator implements BloomFilterCreator {
  public static final int TYPE_VALUE = 1;
  public static final int VERSION = 1;

  private final File _bloomFilterFile;
  private final BloomFilter<String> _bloomFilter;

  public OnHeapGuavaBloomFilterCreator(File indexDir, String columnName, int cardinality,
      BloomFilterConfig bloomFilterConfig) {
    _bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);
    _bloomFilter =
        BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, bloomFilterConfig.getFpp());
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
