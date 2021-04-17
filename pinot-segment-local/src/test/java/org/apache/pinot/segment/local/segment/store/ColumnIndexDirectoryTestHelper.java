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
package org.apache.pinot.segment.local.segment.store;

import java.io.IOException;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;


public class ColumnIndexDirectoryTestHelper {
  static ColumnIndexType[] indexTypes =
      {ColumnIndexType.DICTIONARY, ColumnIndexType.FORWARD_INDEX, ColumnIndexType.INVERTED_INDEX, ColumnIndexType.BLOOM_FILTER, ColumnIndexType.NULLVALUE_VECTOR};

  static PinotDataBuffer newIndexBuffer(ColumnIndexDirectory columnDirectory, String column, int size, int index)
      throws IOException {
    String columnName = column + "." + index;
    // skip star tree. It's managed differently
    ColumnIndexType indexType = indexTypes[index % indexTypes.length];
    PinotDataBuffer buf = columnDirectory.newBuffer(columnName, indexType, size);
    return buf;
  }

  static PinotDataBuffer getIndexBuffer(ColumnIndexDirectory columnDirectory, String column, int index)
      throws IOException {
    String columnName = column + "." + index;
    // skip star tree
    ColumnIndexType indexType = indexTypes[index % indexTypes.length];
    PinotDataBuffer buf = columnDirectory.getBuffer(columnName, indexType);
    return buf;
  }

  static void verifyMultipleReads(ColumnIndexDirectory columnDirectory, String column, int numIter) throws Exception {
    for (int i = 0; i < numIter; i++) {
      // NOTE: PinotDataBuffer is tracked in the ColumnIndexDirectory. No need to close it here.
      PinotDataBuffer buf = ColumnIndexDirectoryTestHelper.getIndexBuffer(columnDirectory, column, i);
      int numValues = (int) (buf.size() / 4);
      for (int j = 0; j < numValues; ++j) {
        Assert.assertEquals(buf.getInt(j * 4), j, "Inconsistent value at index: " + j);
      }
    }
  }

  static void performMultipleWrites(ColumnIndexDirectory columnDirectory, String column, long size, int numIter)
      throws Exception {
    // size is the size of large buffer...split it into parts
    int bufsize = (int) (size / numIter);
    for (int i = 0; i < numIter; i++) {
      // NOTE: PinotDataBuffer is tracked in the ColumnIndexDirectory. No need to close it here.
      PinotDataBuffer buf = ColumnIndexDirectoryTestHelper.newIndexBuffer(columnDirectory, column, bufsize, i);
      int numValues = bufsize / 4;
      for (int j = 0; j < numValues; j++) {
        buf.putInt(j * 4, j);
      }
    }
  }

  static SegmentMetadataImpl writeMetadata(SegmentVersion version) {
    SegmentMetadataImpl meta = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(meta.getVersion()).thenReturn(version.toString());
    Mockito.when(meta.getSegmentVersion()).thenReturn(version);
    Mockito.when(meta.getDictionaryFileName(ArgumentMatchers.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0] + ".dict";
      }
    });
    Mockito.when(meta.getForwardIndexFileName(ArgumentMatchers.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0] + ".fwd";
      }
    });

    Mockito.when(meta.getBitmapInvertedIndexFileName(ArgumentMatchers.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0] + ".ii";
      }
    });
    Mockito.when(meta.getBloomFilterFileName(ArgumentMatchers.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0] + ".bloom";
      }
    });
    Mockito.when(meta.getNullValueVectorFileName(ArgumentMatchers.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0] + ".nullvalue";
      }
    });
    return meta;
  }
}
