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
package com.linkedin.pinot.core.segment.creator.impl.bloom;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;

public class BloomFilterCreator implements AutoCloseable {

  BloomFilter _bloomFilter;
  File _bloomFilterFile;

  public BloomFilterCreator(File indexDir, FieldSpec fieldSpec, int cardinality, int numDocs, int totalNumberOfEntries) {
    String columnName = fieldSpec.getName();
    _bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);
    _bloomFilter = new BloomFilter(cardinality, .03);

  }

  @Override
  public void close() throws Exception {
    byte[] buffer = BloomFilter.serialize(_bloomFilter);
    try (DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_bloomFilterFile)))) {
      dataOutputStream.write(buffer);
    }
  }

  public void add(Object input) {
    _bloomFilter.add(input.toString());
  }

}
