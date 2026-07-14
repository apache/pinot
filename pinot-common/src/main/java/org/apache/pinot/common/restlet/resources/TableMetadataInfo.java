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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/// Aggregated controller response for table segment metadata.
///
/// Column length and cardinality values are averages across logical segments. Disk size, segment count, and row count
/// are table totals with replicas de-duplicated.
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
  // JSON property name kept as "upsertPartitionToServerPrimaryKeyCountMap" to avoid silent data loss during rolling
  // upgrades where servers and controllers may temporarily run different versions of this class.
  private final Map<Integer, Map<String, Long>> _partitionToServerPrimaryKeyCountMap;
  private final List<ColumnCompressionStatsInfo> _columnCompressionStats;
  private final CompressionStatsSummary _compressionStats;

  @JsonCreator
  public TableMetadataInfo(@JsonProperty("tableName") String tableName,
      @JsonProperty("diskSizeInBytes") long sizeInBytes, @JsonProperty("numSegments") long numSegments,
      @JsonProperty("numRows") long numRows, @JsonProperty("columnLengthMap") Map<String, Double> columnLengthMap,
      @JsonProperty("columnCardinalityMap") Map<String, Double> columnCardinalityMap,
      @JsonProperty("maxNumMultiValuesMap") Map<String, Double> maxNumMultiValuesMap,
      @JsonProperty("columnIndexSizeMap") Map<String, Map<String, Double>> columnIndexSizeMap,
      @JsonProperty("upsertPartitionToServerPrimaryKeyCountMap")
      Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap,
      @JsonProperty("columnCompressionStats") @Nullable
      List<ColumnCompressionStatsInfo> columnCompressionStats,
      @JsonProperty("compressionStats") @Nullable CompressionStatsSummary compressionStats) {
    _tableName = tableName;
    _diskSizeInBytes = sizeInBytes;
    _numSegments = numSegments;
    _numRows = numRows;
    _columnLengthMap = columnLengthMap;
    _columnCardinalityMap = columnCardinalityMap;
    _maxNumMultiValuesMap = maxNumMultiValuesMap;
    _columnIndexSizeMap = columnIndexSizeMap;
    _partitionToServerPrimaryKeyCountMap = partitionToServerPrimaryKeyCountMap;
    _columnCompressionStats = columnCompressionStats != null ? List.copyOf(columnCompressionStats) : null;
    _compressionStats = compressionStats;
  }

  /// Constructor for callers that provide column compression statistics but not the aggregate summary.
  public TableMetadataInfo(String tableName, long sizeInBytes, long numSegments, long numRows,
      Map<String, Double> columnLengthMap, Map<String, Double> columnCardinalityMap,
      Map<String, Double> maxNumMultiValuesMap, Map<String, Map<String, Double>> columnIndexSizeMap,
      Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap,
      @Nullable List<ColumnCompressionStatsInfo> columnCompressionStats) {
    this(tableName, sizeInBytes, numSegments, numRows, columnLengthMap, columnCardinalityMap, maxNumMultiValuesMap,
        columnIndexSizeMap, partitionToServerPrimaryKeyCountMap, columnCompressionStats, null);
  }

  /// Backwards-compatible constructor for callers that do not provide compression fields.
  public TableMetadataInfo(String tableName, long sizeInBytes, long numSegments, long numRows,
      Map<String, Double> columnLengthMap, Map<String, Double> columnCardinalityMap,
      Map<String, Double> maxNumMultiValuesMap, Map<String, Map<String, Double>> columnIndexSizeMap,
      Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap) {
    this(tableName, sizeInBytes, numSegments, numRows, columnLengthMap, columnCardinalityMap, maxNumMultiValuesMap,
        columnIndexSizeMap, partitionToServerPrimaryKeyCountMap, null, null);
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

  @JsonProperty("upsertPartitionToServerPrimaryKeyCountMap")
  public Map<Integer, Map<String, Long>> getPartitionToServerPrimaryKeyCountMap() {
    return _partitionToServerPrimaryKeyCountMap;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ColumnCompressionStatsInfo> getColumnCompressionStats() {
    return _columnCompressionStats;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public CompressionStatsSummary getCompressionStats() {
    return _compressionStats;
  }
}
