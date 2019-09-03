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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.JsonUtils;


@SuppressWarnings("unused") // Suppress incorrect warning, as methods are used for json ser/de.
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentPartitionConfig {
  public static final int INVALID_NUM_PARTITIONS = -1;

  @ConfigKey("columnPartitionMap")
  @UseChildKeyHandler(ColumnPartitionMapChildKeyHandler.class)
  private final Map<String, ColumnPartitionConfig> _columnPartitionMap;

  public SegmentPartitionConfig() {
    _columnPartitionMap = null;
  }

  public SegmentPartitionConfig(
      @Nonnull @JsonProperty("columnPartitionMap") Map<String, ColumnPartitionConfig> columnPartitionMap) {
    Preconditions.checkNotNull(columnPartitionMap);
    _columnPartitionMap = columnPartitionMap;
  }

  public Map<String, ColumnPartitionConfig> getColumnPartitionMap() {
    return _columnPartitionMap;
  }

  /**
   * Returns the partition function for the given column, null if there isn't one.
   *
   * @param column Column for which to return the partition function.
   * @return Partition function for the column.
   */
  @Nullable
  public String getFunctionName(@Nonnull String column) {
    ColumnPartitionConfig columnPartitionConfig = _columnPartitionMap.get(column);
    return (columnPartitionConfig != null) ? columnPartitionConfig.getFunctionName() : null;
  }

  /**
   * Set the number of partitions for the specified column.
   *
   * @param column Column for which to set the number of partitions.
   * @param numPartitions Number of partitions to set.
   */
  @JsonIgnore
  public void setNumPartitions(String column, int numPartitions) {
    _columnPartitionMap.get(column).setNumPartitions(numPartitions);
  }

  /**
   * Set the number of partitions for all columns.
   *
   * @param numPartitions Number of partitions.
   */
  @JsonIgnore
  public void setNumPartitions(int numPartitions) {
    for (ColumnPartitionConfig columnPartitionConfig : _columnPartitionMap.values()) {
      columnPartitionConfig.setNumPartitions(numPartitions);
    }
  }

  /**
   * Given a JSON string, de-serialize and return an instance of {@link SegmentPartitionConfig}
   *
   * @param jsonString Input JSON string
   * @return Instance of {@link SegmentPartitionConfig} built from the input string.
   * @throws IOException
   */
  public static SegmentPartitionConfig fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, SegmentPartitionConfig.class);
  }

  /**
   * Returns the JSON equivalent of the object.
   *
   * @return JSON string equivalent of the object.
   * @throws IOException
   */
  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  /**
   * Returns the number of partitions for the specified column.
   * Returns {@link #INVALID_NUM_PARTITIONS} if it does not exist for the column.
   *
   * @param column Column for which to get number of partitions.
   * @return Number of partitions of the column.
   */
  public int getNumPartitions(String column) {
    ColumnPartitionConfig config = _columnPartitionMap.get(column);
    return (config != null) ? config.getNumPartitions() : INVALID_NUM_PARTITIONS;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    SegmentPartitionConfig that = (SegmentPartitionConfig) o;

    return EqualityUtils.isEqual(_columnPartitionMap, that._columnPartitionMap);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_columnPartitionMap);
    return result;
  }
}
