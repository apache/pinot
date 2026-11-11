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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Guards the server-first half of rolling upgrades with DTOs frozen to the pre-compression response shapes.
public class ServerTransportCompatibilityTest {
  @Test
  public void testCurrentTableSizePayloadDeserializesWithLegacyDto()
      throws Exception {
    TableSizeInfo current = new TableSizeInfo("myTable_OFFLINE", 120L,
        List.of(new SegmentSizeInfo("segment0", 120L, 1_000L, 100L, Map.of())),
        TableSizeInfo.CURRENT_METADATA_VERSION);

    LegacyTableSizeInfo legacy =
        JsonUtils.stringToObject(JsonUtils.objectToString(current), LegacyTableSizeInfo.class);

    assertEquals(legacy._tableName, "myTable_OFFLINE");
    assertEquals(legacy._diskSizeInBytes, 120L);
    assertEquals(legacy._segments.size(), 1);
    assertEquals(legacy._segments.get(0)._segmentName, "segment0");
    assertEquals(legacy._segments.get(0)._diskSizeInBytes, 120L);
  }

  @Test
  public void testCurrentTableMetadataPayloadDeserializesWithLegacyDto()
      throws Exception {
    ServerTableMetadataInfo current = ServerTableMetadataInfo.builder("myTable_OFFLINE")
        .withDiskSizeInBytes(120L)
        .withNumSegments(2L)
        .withNumRows(30L)
        .withColumnLengthMap(Map.of("col", 4.0))
        .withColumnCardinalityMap(Map.of("col", 5.0))
        .withMaxNumMultiValuesMap(Map.of("col", 1.0))
        .withColumnIndexSizeMap(Map.of("col", Map.of("forward_index", 12.0)))
        .withPartitionToServerPrimaryKeyCountMap(Map.of(0, Map.of("server0", 10L)))
        .build();

    LegacyTableMetadataInfo legacy =
        JsonUtils.stringToObject(JsonUtils.objectToString(current), LegacyTableMetadataInfo.class);

    assertEquals(legacy._tableName, "myTable_OFFLINE");
    assertEquals(legacy._diskSizeInBytes, 120L);
    assertEquals(legacy._numSegments, 2L);
    assertEquals(legacy._numRows, 30L);
    assertEquals(legacy._columnLengthMap, Map.of("col", 4.0));
    assertEquals(legacy._columnCardinalityMap, Map.of("col", 5.0));
    assertEquals(legacy._maxNumMultiValuesMap, Map.of("col", 1.0));
    assertEquals(legacy._columnIndexSizeMap, Map.of("col", Map.of("forward_index", 12.0)));
    assertEquals(legacy._partitionToServerPrimaryKeyCountMap, Map.of(0, Map.of("server0", 10L)));
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class LegacyTableSizeInfo {
    private final String _tableName;
    private final long _diskSizeInBytes;
    private final List<LegacySegmentSizeInfo> _segments;

    @JsonCreator
    private LegacyTableSizeInfo(@JsonProperty("tableName") String tableName,
        @JsonProperty("diskSizeInBytes") long diskSizeInBytes,
        @JsonProperty("segments") List<LegacySegmentSizeInfo> segments) {
      _tableName = tableName;
      _diskSizeInBytes = diskSizeInBytes;
      _segments = segments;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class LegacySegmentSizeInfo {
    private final String _segmentName;
    private final long _diskSizeInBytes;

    @JsonCreator
    private LegacySegmentSizeInfo(@JsonProperty("segmentName") String segmentName,
        @JsonProperty("diskSizeInBytes") long diskSizeInBytes) {
      _segmentName = segmentName;
      _diskSizeInBytes = diskSizeInBytes;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class LegacyTableMetadataInfo {
    private final String _tableName;
    private final long _diskSizeInBytes;
    private final long _numSegments;
    private final long _numRows;
    private final Map<String, Double> _columnLengthMap;
    private final Map<String, Double> _columnCardinalityMap;
    private final Map<String, Double> _maxNumMultiValuesMap;
    private final Map<String, Map<String, Double>> _columnIndexSizeMap;
    private final Map<Integer, Map<String, Long>> _partitionToServerPrimaryKeyCountMap;

    @JsonCreator
    private LegacyTableMetadataInfo(@JsonProperty("tableName") String tableName,
        @JsonProperty("diskSizeInBytes") long diskSizeInBytes, @JsonProperty("numSegments") long numSegments,
        @JsonProperty("numRows") long numRows,
        @JsonProperty("columnLengthMap") Map<String, Double> columnLengthMap,
        @JsonProperty("columnCardinalityMap") Map<String, Double> columnCardinalityMap,
        @JsonProperty("maxNumMultiValuesMap") Map<String, Double> maxNumMultiValuesMap,
        @JsonProperty("columnIndexSizeMap") Map<String, Map<String, Double>> columnIndexSizeMap,
        @JsonProperty("upsertPartitionToServerPrimaryKeyCountMap")
        Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap) {
      _tableName = tableName;
      _diskSizeInBytes = diskSizeInBytes;
      _numSegments = numSegments;
      _numRows = numRows;
      _columnLengthMap = columnLengthMap;
      _columnCardinalityMap = columnCardinalityMap;
      _maxNumMultiValuesMap = maxNumMultiValuesMap;
      _columnIndexSizeMap = columnIndexSizeMap;
      _partitionToServerPrimaryKeyCountMap = partitionToServerPrimaryKeyCountMap;
    }
  }
}
