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
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;


/// Server-to-controller transport for table metadata contributions.
///
/// Values in this server-to-controller transport DTO are server-local totals. Construction takes ownership of supplied
/// maps and exposes unmodifiable views without copying request-local data. Instances are thread-safe after construction
/// provided callers honor that ownership transfer.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ServerTableMetadataInfo {
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
  private ServerTableMetadataInfo(@JsonProperty("tableName") String tableName,
      @JsonProperty("diskSizeInBytes") long sizeInBytes, @JsonProperty("numSegments") long numSegments,
      @JsonProperty("numRows") long numRows,
      @JsonProperty("columnLengthMap") @Nullable Map<String, Double> columnLengthMap,
      @JsonProperty("columnCardinalityMap") @Nullable Map<String, Double> columnCardinalityMap,
      @JsonProperty("maxNumMultiValuesMap") @Nullable Map<String, Double> maxNumMultiValuesMap,
      @JsonProperty("columnIndexSizeMap") @Nullable Map<String, Map<String, Double>> columnIndexSizeMap,
      @JsonProperty("upsertPartitionToServerPrimaryKeyCountMap")
      @Nullable Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap) {
    _tableName = tableName;
    _diskSizeInBytes = sizeInBytes;
    _numSegments = numSegments;
    _numRows = numRows;
    _columnLengthMap = unmodifiableView(columnLengthMap);
    _columnCardinalityMap = unmodifiableView(columnCardinalityMap);
    _maxNumMultiValuesMap = unmodifiableView(maxNumMultiValuesMap);
    _columnIndexSizeMap = nestedUnmodifiableView(columnIndexSizeMap);
    _partitionToServerPrimaryKeyCountMap = nestedUnmodifiableView(partitionToServerPrimaryKeyCountMap);
  }

  public static Builder builder(String tableName) {
    return new Builder(tableName);
  }

  /// Returns the typed table name.
  public String getTableName() {
    return _tableName;
  }

  /// Returns server-local segment bytes.
  public long getDiskSizeInBytes() {
    return _diskSizeInBytes;
  }

  /// Returns the number of server-local segments represented.
  public long getNumSegments() {
    return _numSegments;
  }

  /// Returns the number of rows across server-local segments.
  public long getNumRows() {
    return _numRows;
  }

  /// Returns summed per-column maximum lengths.
  public Map<String, Double> getColumnLengthMap() {
    return _columnLengthMap;
  }

  /// Returns summed per-column cardinalities.
  public Map<String, Double> getColumnCardinalityMap() {
    return _columnCardinalityMap;
  }

  /// Returns summed per-column maximum multi-value counts.
  public Map<String, Double> getMaxNumMultiValuesMap() {
    return _maxNumMultiValuesMap;
  }

  /// Returns summed per-column index sizes.
  public Map<String, Map<String, Double>> getColumnIndexSizeMap() {
    return _columnIndexSizeMap;
  }

  /// Returns primary-key counts by partition and server.
  @JsonProperty("upsertPartitionToServerPrimaryKeyCountMap")
  public Map<Integer, Map<String, Long>> getPartitionToServerPrimaryKeyCountMap() {
    return _partitionToServerPrimaryKeyCountMap;
  }

  private static <K, V> Map<K, V> unmodifiableView(@Nullable Map<K, V> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    return Collections.unmodifiableMap(source);
  }

  private static <K, N, V> Map<K, Map<N, V>> nestedUnmodifiableView(
      @Nullable Map<K, Map<N, V>> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    return Collections.unmodifiableMap(
        Maps.transformValues(source, value -> Collections.unmodifiableMap(value)));
  }

  /// Mutable, non-thread-safe builder for [ServerTableMetadataInfo].
  public static final class Builder {
    private final String _tableName;
    private long _diskSizeInBytes;
    private long _numSegments;
    private long _numRows;
    private Map<String, Double> _columnLengthMap = Map.of();
    private Map<String, Double> _columnCardinalityMap = Map.of();
    private Map<String, Double> _maxNumMultiValuesMap = Map.of();
    private Map<String, Map<String, Double>> _columnIndexSizeMap = Map.of();
    private Map<Integer, Map<String, Long>> _partitionToServerPrimaryKeyCountMap = Map.of();

    private Builder(String tableName) {
      _tableName = tableName;
    }

    public Builder withDiskSizeInBytes(long diskSizeInBytes) {
      _diskSizeInBytes = diskSizeInBytes;
      return this;
    }

    public Builder withNumSegments(long numSegments) {
      _numSegments = numSegments;
      return this;
    }

    public Builder withNumRows(long numRows) {
      _numRows = numRows;
      return this;
    }

    public Builder withColumnLengthMap(Map<String, Double> columnLengthMap) {
      _columnLengthMap = columnLengthMap;
      return this;
    }

    public Builder withColumnCardinalityMap(Map<String, Double> columnCardinalityMap) {
      _columnCardinalityMap = columnCardinalityMap;
      return this;
    }

    public Builder withMaxNumMultiValuesMap(Map<String, Double> maxNumMultiValuesMap) {
      _maxNumMultiValuesMap = maxNumMultiValuesMap;
      return this;
    }

    public Builder withColumnIndexSizeMap(Map<String, Map<String, Double>> columnIndexSizeMap) {
      _columnIndexSizeMap = columnIndexSizeMap;
      return this;
    }

    public Builder withPartitionToServerPrimaryKeyCountMap(
        Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap) {
      _partitionToServerPrimaryKeyCountMap = partitionToServerPrimaryKeyCountMap;
      return this;
    }

    public ServerTableMetadataInfo build() {
      return new ServerTableMetadataInfo(_tableName, _diskSizeInBytes, _numSegments, _numRows, _columnLengthMap,
          _columnCardinalityMap, _maxNumMultiValuesMap, _columnIndexSizeMap,
          _partitionToServerPrimaryKeyCountMap);
    }
  }
}
