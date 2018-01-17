/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SortedIndexReaderTest {

  private int _cardinality = 100000;
  private final static File file  = new File(FileUtils.getTempDirectory(), "SortedIndex");
  private int[] startDocIdArray;
  private int[] endDocIdArray;

  @BeforeClass
  public void setUp() throws IOException {
    int maxLength = 1000;
    FileUtils.deleteQuietly(file);
    int[] columnSizes = new int[] {
        4, 4
    };
    FixedByteSingleValueMultiColWriter writer =
        new FixedByteSingleValueMultiColWriter(file, _cardinality, columnSizes.length, columnSizes);
    Random random = new Random();
    startDocIdArray = new int[_cardinality];
    endDocIdArray = new int[_cardinality];
    int prevEnd = -1;
    for (int i = 0; i < _cardinality; i++) {
      int length = random.nextInt(maxLength);
      int start = prevEnd + 1;
      int end = start + length;
      startDocIdArray[i] = start;
      endDocIdArray[i] = end;
      writer.setInt(i, 0, start);
      writer.setInt(i, 1, end);
      prevEnd = end;
    }
    writer.close();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testSortedForwardIndex() throws Exception {
    try (PinotDataBuffer heapBuffer =
        PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing");
        SortedIndexReader reader = new SortedIndexReader(heapBuffer, _cardinality)) {
      // without using context
      for (int i = 0; i < _cardinality; i++) {
        for (int docId = startDocIdArray[i]; docId <= endDocIdArray[i]; docId++) {
          Assert.assertEquals(reader.getInt(docId), i);
        }
      }
      // with context
      SortedIndexReader.Context context = reader.createContext();
      for (int i = 0; i < _cardinality; i++) {
        for (int docId = startDocIdArray[i]; docId <= endDocIdArray[i]; docId++) {
          Assert.assertEquals(reader.getInt(docId, context), i);
        }
      }
    }
  }

  @Test
  public void testSortedInvertedIndex() throws IOException {
    try (PinotDataBuffer heapBuffer =
        PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing");
        SortedIndexReader reader = new SortedIndexReader(heapBuffer, _cardinality)) {
      for (int i = 0; i < _cardinality; i++) {
        Pairs.IntPair intPair = reader.getDocIds(i);
        Assert.assertEquals(intPair.getLeft(), startDocIdArray[i]);
        Assert.assertEquals(intPair.getRight(), endDocIdArray[i]);
      }
    }
  }
}
