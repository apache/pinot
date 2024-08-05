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
package org.apache.pinot.segment.local.dedup;

import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DedupTestUtils {
  public static final String RAW_TABLE_NAME = "testTable";
  public static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private DedupTestUtils() {
  }

  public static DedupUtils.DedupRecordInfoReader generateDedupRecordInfoReader(int numberOfDocs,
      int startPrimaryKeyValue) {
    PrimaryKeyReader primaryKeyReader = Mockito.mock(PrimaryKeyReader.class);
    PinotSegmentColumnReader dedupTimeColumnReader = Mockito.mock(PinotSegmentColumnReader.class);
    for (int i = 0; i < numberOfDocs; i++) {
      int primaryKeyValue = startPrimaryKeyValue + i;
      Mockito.when(primaryKeyReader.getPrimaryKey(i)).thenReturn(DedupTestUtils.getPrimaryKey(primaryKeyValue));
      double time = primaryKeyValue * 1000;
      Mockito.when(dedupTimeColumnReader.getValue(i)).thenReturn(time);
    }
    return new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
  }

  public static ImmutableSegmentImpl mockSegment(int sequenceNumber, int totalDocs) {
    // Mock the segment name
    ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
    String segmentName = getSegmentName(sequenceNumber);
    when(segment.getSegmentName()).thenReturn(segmentName);
    // Mock the segment total doc
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    return segment;
  }

  public static String getSegmentName(int sequenceNumber) {
    return new LLCSegmentName(RAW_TABLE_NAME, 0, sequenceNumber, System.currentTimeMillis()).toString();
  }

  public static PrimaryKey getPrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }
}
