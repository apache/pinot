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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSetV2;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedBitSVForwardIndexReaderV2Test {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FixedBitIntReaderTest");
  private static final int NUM_VALUES = 99_999;
  private static final int NUM_DOC_IDS = PinotDataBitSetV2.MAX_DOC_PER_CALL;
  private static final Random RANDOM = new Random();

  private final int[][] _sequentialDocIds = new int[32][NUM_DOC_IDS];
  private final int[] _sparseDocIds = new int[NUM_DOC_IDS];
  private final int[] _lastSequentialDocIds = new int[NUM_DOC_IDS];

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);

    for (int i = 0; i < 32; i++) {
      int[] sequentialDocIds = new int[NUM_DOC_IDS];
      _sequentialDocIds[i] = sequentialDocIds;
      for (int j = 0; j < NUM_DOC_IDS; j++) {
        sequentialDocIds[j] = i + j;
      }
    }

    int sparseDocId = RANDOM.nextInt(10);
    for (int i = 0; i < NUM_DOC_IDS; i++) {
      _sparseDocIds[i] = sparseDocId;
      sparseDocId += 5 + RANDOM.nextInt(6);
      _lastSequentialDocIds[i] = NUM_VALUES - NUM_DOC_IDS + i;
    }
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
    int[] dictIdBuffer = new int[NUM_VALUES];
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
        FixedBitSVForwardIndexReaderV2 reader = new FixedBitSVForwardIndexReaderV2(dataBuffer, NUM_VALUES, numBits);
        for (int i = 0; i < NUM_VALUES; i++) {
          assertEquals(reader.getDictId(i, null), values[i]);
        }
        for (int i = 0; i < 32; i++) {
          int[] sequentialDocIds = _sequentialDocIds[i];
          reader.readDictIds(sequentialDocIds, NUM_DOC_IDS, dictIdBuffer, null);
          for (int j = 0; j < NUM_DOC_IDS; j++) {
            Assert.assertEquals(dictIdBuffer[j], values[sequentialDocIds[j]]);
          }
        }
        reader.readDictIds(_sparseDocIds, NUM_DOC_IDS, dictIdBuffer, null);
        for (int i = 0; i < NUM_DOC_IDS; i++) {
          Assert.assertEquals(dictIdBuffer[i], values[_sparseDocIds[i]]);
        }
        reader.readDictIds(_lastSequentialDocIds, NUM_DOC_IDS, dictIdBuffer, null);
        for (int i = 0; i < NUM_DOC_IDS; i++) {
          Assert.assertEquals(dictIdBuffer[i], values[_lastSequentialDocIds[i]]);
        }
      }

      // Value range test
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          FixedBitSVForwardIndexReader reader = new FixedBitSVForwardIndexReader(dataBuffer, NUM_VALUES,
              numBits)) {
        Assert.assertTrue(reader.isFixedOffsetMappingType());
        Assert.assertEquals(reader.getRawDataStartOffset(), 0);
        Assert.assertEquals(reader.getDocLength(), numBits);
        Assert.assertTrue(reader.isDocLengthInBits());

        try {
          reader.recordDocIdByteRanges(0, null, new ArrayList<>());
          Assert.fail("Should have failed to record byte ranges");
        } catch (UnsupportedOperationException e) {
          // expected
          Assert.assertEquals(e.getMessage(), "Forward index is fixed length type");
        }
      }
    }
  }
}
