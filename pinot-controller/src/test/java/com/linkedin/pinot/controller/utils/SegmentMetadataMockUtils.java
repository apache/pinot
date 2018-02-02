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
package com.linkedin.pinot.controller.utils;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import org.mockito.Mockito;


public class SegmentMetadataMockUtils {
  private SegmentMetadataMockUtils() {
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, String segmentName, int numTotalDocs,
      String crc) {
    SegmentMetadata segmentMetadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(segmentMetadata.getTableName()).thenReturn(tableName);
    Mockito.when(segmentMetadata.getName()).thenReturn(segmentName);
    Mockito.when(segmentMetadata.getTotalDocs()).thenReturn(numTotalDocs);
    Mockito.when(segmentMetadata.getTotalRawDocs()).thenReturn(numTotalDocs);
    Mockito.when(segmentMetadata.getCrc()).thenReturn(crc);
    return segmentMetadata;
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, tableName + uniqueNumericString, 0, uniqueNumericString);
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, String segmentName) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, segmentName, 0, uniqueNumericString);
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, int numTotalDocs) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, tableName + uniqueNumericString, numTotalDocs, uniqueNumericString);
  }

  public static SegmentMetadata mockSegmentMetadata(String tableName, String segmentName, int numTotalDocs) {
    String uniqueNumericString = Long.toString(System.nanoTime());
    return mockSegmentMetadata(tableName, segmentName, numTotalDocs, uniqueNumericString);
  }
}
