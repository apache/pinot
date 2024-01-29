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
import java.util.Map;


/**
 * Class to represent aggregated segment metadata for a table.
 * - When return by server API, columnLengthMap/columnCardinalityMap is the total columnLength/columnCardinality across
 *   all segments on that server.
 * - When return by controller API, columnLengthMap/columnCardinalityMap is the average columnLength/columnCardinality
 *   across all segments for the given table.
 * - For diskSizeInBytes/numSegments/numRows, both server API and controller API will return the total value across all
 *   segments (if a segment has multiple replicas, only consider one replica).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetadataInfo {
  private final String _tableName;
  private final long _diskSizeInBytes;
  private final long _numSegments;
  private final long _numRows;
  private final Map<String, Double> _columnLengthMap;
  private final Map<String, Double> _columnCardinalityMap;
  private final Map<String, Double> _maxNumMultiValuesMap;
  private final Map<String, Map<String, Double>> _columnIndexSizeMap;
  private final Map<Integer, Long> _upsertPartitionToPrimaryKeyCountMap;

  @JsonCreator
  public TableMetadataInfo(@JsonProperty("tableName") String tableName,
      @JsonProperty("diskSizeInBytes") long sizeInBytes, @JsonProperty("numSegments") long numSegments,
      @JsonProperty("numRows") long numRows, @JsonProperty("columnLengthMap") Map<String, Double> columnLengthMap,
      @JsonProperty("columnCardinalityMap") Map<String, Double> columnCardinalityMap,
      @JsonProperty("maxNumMultiValuesMap") Map<String, Double> maxNumMultiValuesMap,
      @JsonProperty("columnIndexSizeMap") Map<String, Map<String, Double>> columnIndexSizeMap,
      @JsonProperty("upsertPartitionToPrimaryKeyCountMap") Map<Integer, Long> upsertPartitionToPrimaryKeyCountMap) {
    _tableName = tableName;
    _diskSizeInBytes = sizeInBytes;
    _numSegments = numSegments;
    _numRows = numRows;
    _columnLengthMap = columnLengthMap;
    _columnCardinalityMap = columnCardinalityMap;
    _maxNumMultiValuesMap = maxNumMultiValuesMap;
    _columnIndexSizeMap = columnIndexSizeMap;
    _upsertPartitionToPrimaryKeyCountMap = upsertPartitionToPrimaryKeyCountMap;
  }

  public String getTableName() {
    return _tableName;
  }

  public long getDiskSizeInBytes() {
    return _diskSizeInBytes;
  }

  public long getNumSegments() {
    return _numSegments;
  }

  public long getNumRows() {
    return _numRows;
  }

  public Map<String, Double> getColumnLengthMap() {
    return _columnLengthMap;
  }

  public Map<String, Double> getColumnCardinalityMap() {
    return _columnCardinalityMap;
  }

  public Map<String, Double> getMaxNumMultiValuesMap() {
    return _maxNumMultiValuesMap;
  }

  public Map<String, Map<String, Double>> getColumnIndexSizeMap() {
    return _columnIndexSizeMap;
  }

  public Map<Integer, Long> getUpsertPartitionToPrimaryKeyCountMap() {
    return _upsertPartitionToPrimaryKeyCountMap;
  }
}
