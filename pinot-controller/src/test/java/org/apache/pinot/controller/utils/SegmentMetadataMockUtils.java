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
package org.apache.pinot.controller.utils;

import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.MurmurPartitionFunction;
import org.joda.time.Interval;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentMetadataMockUtils {
  private SegmentMetadataMockUtils() {
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, String segmentName, int numTotalDocs,
      String crc) {
    SegmentMetadata segmentMetadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(segmentMetadata.getTableName()).thenReturn(tableName);
    Mockito.when(segmentMetadata.getName()).thenReturn(segmentName);
    Mockito.when(segmentMetadata.getTotalDocs()).thenReturn(numTotalDocs);
    Mockito.when(segmentMetadata.getCrc()).thenReturn(crc);
    Mockito.when(segmentMetadata.getStartTime()).thenReturn(1L);
    Mockito.when(segmentMetadata.getEndTime()).thenReturn(10L);
    Mockito.when(segmentMetadata.getTimeInterval()).thenReturn(
        new Interval(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.DAYS)));
    Mockito.when(segmentMetadata.getTimeUnit()).thenReturn(TimeUnit.DAYS);
    return segmentMetadata;
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, tableName + uniqueNumericString, 100, uniqueNumericString);
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, String segmentName) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, segmentName, 100, uniqueNumericString);
  }

  public static SegmentZKMetadata mockSegmentZKMetadata(String segmentName, long numTotalDocs) {
    SegmentZKMetadata segmentZKMetadata = Mockito.mock(SegmentZKMetadata.class);
    Mockito.when(segmentZKMetadata.getSegmentName()).thenReturn(segmentName);
    Mockito.when(segmentZKMetadata.getTotalDocs()).thenReturn(numTotalDocs);
    return segmentZKMetadata;
  }

  public static SegmentMetadata mockSegmentMetadataWithPartitionInfo(String rawTableName, String segmentName,
      String columnName, int partitionNumber) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    Set<Integer> partitions = Collections.singleton(partitionNumber);
    when(columnMetadata.getPartitions()).thenReturn(partitions);
    when(columnMetadata.getPartitionFunction()).thenReturn(new MurmurPartitionFunction(5));

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    if (columnName != null) {
      when(segmentMetadata.getColumnMetadataFor(columnName)).thenReturn(columnMetadata);
    }
    when(segmentMetadata.getTableName()).thenReturn(rawTableName);
    when(segmentMetadata.getName()).thenReturn(segmentName);
    when(segmentMetadata.getCrc()).thenReturn("0");

    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put(columnName, columnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);
    return segmentMetadata;
  }

  public static SegmentMetadata mockSegmentMetadataWithEndTimeInfo(String tableName, String segmentName, long endTime) {
    SegmentMetadata segmentMetadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(segmentMetadata.getTableName()).thenReturn(tableName);
    Mockito.when(segmentMetadata.getName()).thenReturn(segmentName);
    Mockito.when(segmentMetadata.getTotalDocs()).thenReturn(10);
    Mockito.when(segmentMetadata.getCrc()).thenReturn(Long.toString(System.nanoTime()));
    Mockito.when(segmentMetadata.getStartTime()).thenReturn(endTime - 10);
    Mockito.when(segmentMetadata.getEndTime()).thenReturn(endTime);
    Mockito.when(segmentMetadata.getTimeInterval()).thenReturn(
        new Interval(TimeUnit.MILLISECONDS.convert(endTime - 10, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(endTime, TimeUnit.DAYS)));
    Mockito.when(segmentMetadata.getTimeUnit()).thenReturn(TimeUnit.DAYS);
    return segmentMetadata;
  }
}
