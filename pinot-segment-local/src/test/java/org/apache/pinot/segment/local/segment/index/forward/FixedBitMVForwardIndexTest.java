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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitMVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedBitMVForwardIndexTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "FixedBitMVForwardIndexTest");
  private static final File INDEX_FILE =
      new File(TEMP_DIR, "testColumn" + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
  private static final int NUM_DOCS = 100;
  private static final int MAX_NUM_VALUES_PER_MV_ENTRY = 100;
  private static final Random RANDOM = new Random();

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @Test
  public void testFixedBitMVForwardIndex()
      throws Exception {
    for (int numBitsPerValue = 1; numBitsPerValue <= 31; numBitsPerValue++) {
      // Generate random values
      int[][] valuesArray = new int[NUM_DOCS][];
      int totalNumValues = 0;
      int maxValue = numBitsPerValue != 31 ? 1 << numBitsPerValue : Integer.MAX_VALUE;
      for (int i = 0; i < NUM_DOCS; i++) {
        int numValues = RANDOM.nextInt(MAX_NUM_VALUES_PER_MV_ENTRY) + 1;
        int[] values = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          values[j] = RANDOM.nextInt(maxValue);
        }
        valuesArray[i] = values;
        totalNumValues += numValues;
      }

      // Create the forward index
      try (FixedBitMVForwardIndexWriter writer = new FixedBitMVForwardIndexWriter(INDEX_FILE, NUM_DOCS, totalNumValues,
          numBitsPerValue)) {
        for (int[] values : valuesArray) {
          writer.putDictIds(values);
        }
      }

      // Read the forward index
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(INDEX_FILE);
          FixedBitMVForwardIndexReader reader = new FixedBitMVForwardIndexReader(dataBuffer, NUM_DOCS, totalNumValues,
              numBitsPerValue); FixedBitMVForwardIndexReader.Context readerContext = reader.createContext()) {
        int[] valueBuffer = new int[MAX_NUM_VALUES_PER_MV_ENTRY];
        for (int i = 0; i < NUM_DOCS; i++) {
          int numValues = reader.getDictIdMV(i, valueBuffer, readerContext);
          for (int j = 0; j < numValues; j++) {
            assertEquals(valueBuffer[j], valuesArray[i][j]);
          }
        }
      }

      // Byte range test
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(INDEX_FILE);
          FixedBitMVForwardIndexReader reader = new FixedBitMVForwardIndexReader(dataBuffer, NUM_DOCS, totalNumValues,
              numBitsPerValue); FixedBitMVForwardIndexReader.Context readerContext = reader.createContext()) {
        Assert.assertTrue(reader.isBufferByteRangeInfoSupported());
        Assert.assertFalse(reader.isFixedOffsetMappingType());
        List<ForwardIndexReader.ByteRange> rangeList = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
          try {
            reader.recordDocIdByteRanges(i, readerContext, rangeList);
          } catch (Exception e) {
            Assert.fail("Should not throw exception when calling recordDocIdByteRanges()", e);
          }
        }
      }

      FileUtils.forceDelete(INDEX_FILE);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
