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
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.mockito.Mockito;
import org.testng.Assert;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;


public class ColumnIndexDirectoryTestHelper {
  private ColumnIndexDirectoryTestHelper() {
  }

  private static final IndexType[] INDEX_TYPES = {
     StandardIndexes.dictionary(), StandardIndexes.forward(), StandardIndexes.inverted(),
      StandardIndexes.bloomFilter(), StandardIndexes.nullValueVector()
  };

  static PinotDataBuffer newIndexBuffer(ColumnIndexDirectory columnDirectory, String column, int size, int index)
      throws IOException {
    String columnName = column + "." + index;
    // skip star tree. It's managed differently
    IndexType indexType = INDEX_TYPES[index % INDEX_TYPES.length];
    PinotDataBuffer buf = columnDirectory.newBuffer(columnName, indexType, size);
    return buf;
  }

  static PinotDataBuffer getIndexBuffer(ColumnIndexDirectory columnDirectory, String column, int index)
      throws IOException {
    String columnName = column + "." + index;
    // skip star tree
    IndexType indexType = INDEX_TYPES[index % INDEX_TYPES.length];
    PinotDataBuffer buf = columnDirectory.getBuffer(columnName, indexType);
    return buf;
  }

  static void verifyMultipleReads(ColumnIndexDirectory columnDirectory, String column, int numIter)
      throws Exception {
    for (int i = 0; i < numIter; i++) {
      // NOTE: PinotDataBuffer is tracked in the ColumnIndexDirectory. No need to close it here.
      PinotDataBuffer buf = ColumnIndexDirectoryTestHelper.getIndexBuffer(columnDirectory, column, i);
      int numValues = (int) (buf.size() / 4);
      for (int j = 0; j < numValues; j++) {
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
    SegmentMetadataImpl segmentMetadata = Mockito.mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getVersion()).thenReturn(version);
    ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);
    when(columnMetadata.isSingleValue()).thenReturn(true);
    when(columnMetadata.isSorted()).thenReturn(false);
    when(segmentMetadata.getColumnMetadataFor(anyString())).thenReturn(columnMetadata);
    return segmentMetadata;
  }
}
