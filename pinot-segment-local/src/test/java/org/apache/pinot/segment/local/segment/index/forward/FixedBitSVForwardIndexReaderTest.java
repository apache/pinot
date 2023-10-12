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
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class FixedBitSVForwardIndexReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "FixedBitMVForwardIndexTest");
  private static final File INDEX_FILE =
      new File(TEMP_DIR, "testColumn" + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
  private static final int NUM_DOCS = 100;
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
      int[] valuesArray = new int[NUM_DOCS];
      int totalNumValues = 0;
      int maxValue = numBitsPerValue != 31 ? 1 << numBitsPerValue : Integer.MAX_VALUE;
      for (int i = 0; i < NUM_DOCS; i++) {
        valuesArray[i] = RANDOM.nextInt(maxValue);
      }

      // Create the forward index
      try (FixedBitSVForwardIndexWriter writer = new FixedBitSVForwardIndexWriter(INDEX_FILE, NUM_DOCS,
          numBitsPerValue)) {
        for (int value : valuesArray) {
          writer.putDictId(value);
        }
      }

      // Read the forward index
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(INDEX_FILE);
          FixedBitSVForwardIndexReader reader = new FixedBitSVForwardIndexReader(dataBuffer, NUM_DOCS,
              numBitsPerValue)) {
        for (int i = 0; i < NUM_DOCS; i++) {
          assertEquals(valuesArray[i], reader.getDictId(i, null));
        }
      }

      // Byte range test
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(INDEX_FILE);
          FixedBitSVForwardIndexReader reader = new FixedBitSVForwardIndexReader(dataBuffer, NUM_DOCS,
              numBitsPerValue)) {
        Assert.assertTrue(reader.isBufferByteRangeInfoSupported());
        Assert.assertTrue(reader.isFixedOffsetMappingType());
        Assert.assertEquals(reader.getRawDataStartOffset(), 0);
        Assert.assertEquals(reader.getDocLength(), numBitsPerValue);
        Assert.assertTrue(reader.isDocLengthInBits());

        try {
          reader.recordDocIdByteRanges(0, null, new ArrayList<>());
          Assert.fail("Should have failed to record byte ranges");
        } catch (UnsupportedOperationException e) {
          // expected
          Assert.assertEquals(e.getMessage(), "Forward index is fixed length type");
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
