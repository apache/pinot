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
package org.apache.pinot.core.segment.index.creator;

import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.core.bloom.BloomFilterType;
import org.apache.pinot.core.bloom.BloomFilterUtil;
import org.apache.pinot.core.bloom.GuavaOnHeapBloomFilter;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.bloom.BloomFilterCreator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class BloomFilterCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BloomFilterCreatorTest");
  private static int MB_IN_BYTES = 1024 * 1024;

  public void setUp()
      throws Exception {
    if (TEMP_DIR.exists()) {
      FileUtils.deleteQuietly(TEMP_DIR);
    }
    TEMP_DIR.mkdir();
  }

  @Test
  public void testBloomFilterUtil() {
    // Test against the known results
    Assert.assertEquals(BloomFilterUtil.computeNumBits(1000000, 0.03), 7298441);
    Assert.assertEquals(BloomFilterUtil.computeNumBits(10000000, 0.03), 72984409);
    Assert.assertEquals(BloomFilterUtil.computeNumBits(10000000, 0.1), 47925292);

    Assert.assertEquals(BloomFilterUtil.computeNumberOfHashFunctions(1000000, 7298441), 5);
    Assert.assertEquals(BloomFilterUtil.computeNumberOfHashFunctions(10000000, 72984409), 5);
    Assert.assertEquals(BloomFilterUtil.computeNumberOfHashFunctions(10000000, 47925292), 3);

    double threshold = 0.001;
    Assert
        .assertTrue(compareDouble(BloomFilterUtil.computeMaxFalsePosProbability(1000000, 5, 7298441), 0.03, threshold));
    Assert.assertTrue(
        compareDouble(BloomFilterUtil.computeMaxFalsePosProbability(10000000, 5, 72984409), 0.03, threshold));
    Assert.assertTrue(
        compareDouble(BloomFilterUtil.computeMaxFalsePosProbability(10000000, 3, 47925292), 0.1, threshold));
  }

  private boolean compareDouble(double a, double b, double threshold) {
    if (Math.abs(a - b) < threshold) {
      return true;
    }
    return false;
  }

  @Test
  public void testBloomFilterCreator()
      throws Exception {
    // Create bloom filter directory
    File bloomFilterDir = new File(TEMP_DIR, "bloomFilterDir");
    bloomFilterDir.mkdirs();

    // Create a bloom filter and serialize it to a file
    int cardinality = 10000;
    String columnName = "testColumn";
    BloomFilterCreator bloomFilterCreator = new BloomFilterCreator(bloomFilterDir, columnName, cardinality);
    for (int i = 0; i < 5; i++) {
      bloomFilterCreator.add(Integer.toString(i));
    }
    bloomFilterCreator.close();

    // Deserialize the bloom filter and validate
    File bloomFilterFile = new File(bloomFilterDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    try (DataInputStream in = new DataInputStream(new FileInputStream(bloomFilterFile))) {
      BloomFilterType type = BloomFilterType.valueOf(in.readInt());
      int version = in.readInt();
      GuavaOnHeapBloomFilter bloomFilter = new GuavaOnHeapBloomFilter();

      Assert.assertEquals(type, bloomFilter.getBloomFilterType());
      Assert.assertEquals(version, bloomFilter.getVersion());

      bloomFilter.readFrom(in);
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(bloomFilter.mightContain(Integer.toString(i)));
      }
      for (int j = 5; j < 10; j++) {
        Assert.assertFalse(bloomFilter.mightContain(Integer.toString(j)));
      }
    }
  }

  @Test
  public void testBloomFilterSize()
      throws Exception {
    int cardinalityArray[] = new int[]{10, 100, 1000, 100000, 100000, 1000000, 5000000, 10000000};
    for (int cardinality : cardinalityArray) {
      FileUtils.deleteQuietly(TEMP_DIR);
      File indexDir = new File(TEMP_DIR, "testBloomFilterSize");
      Preconditions.checkState(indexDir.mkdirs());

      String columnName = "testSize";
      BloomFilterCreator bloomFilterCreator = new BloomFilterCreator(indexDir, columnName, cardinality);
      bloomFilterCreator.close();

      File bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

      try (InputStream inputStream = new FileInputStream(bloomFilterFile)) {
        byte[] bloomFilterBytes = IOUtils.toByteArray(inputStream);
        long actualBloomFilterSize = bloomFilterBytes.length;
        // Check if the size of bloom filter does not go beyond 1MB. Note that guava bloom filter has 11-12 bytes of
        // overhead
        Assert.assertTrue(actualBloomFilterSize < MB_IN_BYTES + 12);
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
