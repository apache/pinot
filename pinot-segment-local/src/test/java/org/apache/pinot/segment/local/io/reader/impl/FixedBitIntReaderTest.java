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
package org.apache.pinot.segment.local.io.reader.impl;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedBitIntReaderTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FixedBitIntReaderTest");
  private static final int NUM_VALUES = 95;
  private static final Random RANDOM = new Random();

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testFixedBitIntReader()
      throws Exception {
    int[] values = new int[NUM_VALUES];
    for (int numBits = 1; numBits <= 31; numBits++) {
      File indexFile = new File(INDEX_DIR, "bit-" + numBits);
      try (
          FixedBitSVForwardIndexWriter indexWriter = new FixedBitSVForwardIndexWriter(indexFile, NUM_VALUES, numBits)) {
        int maxValue = numBits < 31 ? 1 << numBits : Integer.MAX_VALUE;
        for (int i = 0; i < NUM_VALUES; i++) {
          int value = RANDOM.nextInt(maxValue);
          values[i] = value;
          indexWriter.putDictId(value);
        }
      }
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        FixedBitIntReader intReader = FixedBitIntReader.getReader(dataBuffer, numBits);
        for (int i = 0; i < NUM_VALUES; i++) {
          assertEquals(intReader.read(i), values[i]);
        }
        for (int i = 0; i < NUM_VALUES - 2; i++) {
          assertEquals(intReader.readUnchecked(i), values[i]);
        }
        int[] out = new int[64];
        intReader.read32(0, out, 0);
        intReader.read32(32, out, 32);
        for (int i = 0; i < 64; i++) {
          assertEquals(out[i], values[i]);
        }
      }

      // Validate value range behaviour
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        FixedBitIntReader intReader = FixedBitIntReader.getReader(dataBuffer, numBits);
        for (int i = 0; i < NUM_VALUES; i++) {
          assertEquals(intReader.read(i), values[i]);
        }
        for (int i = 0; i < NUM_VALUES - 2; i++) {
          assertEquals(intReader.readUnchecked(i), values[i]);
        }
        int[] out = new int[64];
        intReader.read32(0, out, 0);
        intReader.read32(32, out, 32);
        for (int i = 0; i < 64; i++) {
          assertEquals(out[i], values[i]);
        }
      }
    }
  }
}
