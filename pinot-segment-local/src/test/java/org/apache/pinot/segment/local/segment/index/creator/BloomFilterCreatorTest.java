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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.index.readers.bloom.BloomFilterReaderFactory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BloomFilterCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BloomFilterCreatorTest");

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
  }

  @Test
  public void testBloomFilterCreator()
      throws Exception {
    // Create the bloom filter
    int cardinality = 10000;
    String columnName = "testColumn";
    try (BloomFilterCreator bloomFilterCreator = new OnHeapGuavaBloomFilterCreator(TEMP_DIR, columnName, cardinality,
        new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 0, false), FieldSpec.DataType.INT)) {
      for (int i = 0; i < 5; i++) {
        bloomFilterCreator.add(Integer.toString(i));
      }
      bloomFilterCreator.seal();
    }

    // Read the bloom filter
    File bloomFilterFile = new File(TEMP_DIR, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(bloomFilterFile);
        BloomFilterReader onHeapBloomFilter = BloomFilterReaderFactory.getBloomFilterReader(dataBuffer, true);
        BloomFilterReader offHeapBloomFilter = BloomFilterReaderFactory.getBloomFilterReader(dataBuffer, false);) {
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(onHeapBloomFilter.mightContain(Integer.toString(i)));
        Assert.assertTrue(offHeapBloomFilter.mightContain(Integer.toString(i)));
      }
      for (int i = 5; i < 10; i++) {
        Assert.assertFalse(onHeapBloomFilter.mightContain(Integer.toString(i)));
        Assert.assertFalse(offHeapBloomFilter.mightContain(Integer.toString(i)));
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
