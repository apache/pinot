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
package com.linkedin.pinot.index.reader;

import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReaderImpl;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SortedForwardIndexReaderTest {
  private static Logger LOGGER = LoggerFactory.getLogger(SortedForwardIndexReaderTest.class);
  public void testSimple() throws Exception {

    int maxLength = 1000;
    int cardinality = 100000;
    File file = new File("test_sortef_fwd_index.dat");
    file.delete();
    int[] columnSizes = new int[] {
        4, 4
    };
    FixedByteSingleValueMultiColWriter writer =
        new FixedByteSingleValueMultiColWriter(file, cardinality, columnSizes.length, columnSizes);
    Random random = new Random();
    int[] startDocIdArray = new int[cardinality];
    int[] endDocIdArray = new int[cardinality];
    int prevEnd = -1;
    int totalDocs = 0;
    for (int i = 0; i < cardinality; i++) {
      int length = random.nextInt(maxLength);
      int start = prevEnd + 1;
      int end = start + length;
      startDocIdArray[i] = start;
      endDocIdArray[i] = end;
      writer.setInt(i, 0, start);
      writer.setInt(i, 1, end);
      prevEnd = end;
      totalDocs += length;
    }
    writer.close();

    try (SortedIndexReader reader = new SortedIndexReaderImpl(PinotDataBuffer.loadBigEndianFile(file), cardinality)) {
      // without using context
      long start, end;
      start = System.currentTimeMillis();
      for (int i = 0; i < cardinality; i++) {
        for (int docId = startDocIdArray[i]; docId <= endDocIdArray[i]; docId++) {
          Assert.assertEquals(reader.getInt(docId), i);
        }
      }
      end = System.currentTimeMillis();
      System.out.println("Took " + (end - start) + " to scan " + totalDocs + " docs without using context");
      // with context
      ReaderContext context = reader.createContext();
      start = System.currentTimeMillis();
      for (int i = 0; i < cardinality; i++) {
        for (int docId = startDocIdArray[i]; docId <= endDocIdArray[i]; docId++) {
          Assert.assertEquals(reader.getInt(docId, context), i);
        }
      }
      end = System.currentTimeMillis();
      LOGGER.debug("Took " + (end - start) + " to scan " + totalDocs + " with context");
    }

    file.delete();
  }
}
