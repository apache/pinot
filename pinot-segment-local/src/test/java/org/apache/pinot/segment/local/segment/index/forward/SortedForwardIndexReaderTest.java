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
import java.util.Random;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.local.segment.index.readers.sorted.SortedIndexReaderImpl;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class SortedForwardIndexReaderTest {

  public void testSimple() throws Exception {

    int maxLength = 1000;
    int cardinality = 100000;
    File file = new File("test_sortef_fwd_index.dat");
    file.delete();
    int[] columnSizes = new int[]{4, 4};
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

    try (SortedIndexReaderImpl reader = new SortedIndexReaderImpl(PinotDataBuffer.loadBigEndianFile(file), cardinality);
        SortedIndexReaderImpl.Context readerContext = reader.createContext()) {
      for (int i = 0; i < cardinality; i++) {
        for (int docId = startDocIdArray[i]; docId <= endDocIdArray[i]; docId++) {
          Assert.assertEquals(reader.getDictId(docId, readerContext), i);
        }
      }
    }

    file.delete();
  }
}
