package com.linkedin.pinot.common.metadata.segment;

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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Class for partition metadata for a segment.
 */
@SuppressWarnings("unused") // Suppress incorrect warning, as methods are used for json ser/de.
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentPartitionMetadata {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final int INVALID_NUM_PARTITIONS = -1;
  private final Map<String, ColumnPartitionMetadata> _columnPartitionMap;

  /**
   * Constructor for the class.
   *
   * @param columnPartitionMap Column name to ColumnPartitionMetadata map.
   */
  public SegmentPartitionMetadata(
      @Nonnull @JsonProperty("columnPartitionMap") Map<String, ColumnPartitionMetadata> columnPartitionMap) {
    Preconditions.checkNotNull(columnPartitionMap);
    _columnPartitionMap = columnPartitionMap;
  }

  /**
   * Returns the map from column name to column's partition metadata.
   *
   * @return Map from column name to its partition metadata.
   */
  public Map<String, ColumnPartitionMetadata> getColumnPartitionMap() {
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
    ColumnPartitionMetadata columnPartitionMetadata = _columnPartitionMap.get(column);
    return (columnPartitionMetadata != null) ? columnPartitionMetadata.getFunctionName() : null;
  }

  /**
   * Set the number of partitions for the specified column.
   *
   * @param column Column for which to set the number of partitions.
   * @param numPartitions Number of partitions to set.
   */
  @JsonIgnore
  public void setNumPartitions(String column, int numPartitions) {
    ColumnPartitionMetadata columnPartitionMetadata = _columnPartitionMap.get(column);
    columnPartitionMetadata.setNumPartitions(numPartitions);
  }

  /**
   * Given a JSON string, de-serialize and return an instance of {@link SegmentPartitionMetadata}
   *
   * @param jsonString Input JSON string
   * @return Instance of {@link SegmentPartitionMetadata} built from the input string.
   * @throws IOException
   */
  public static SegmentPartitionMetadata fromJsonString(String jsonString)
      throws IOException {
    return OBJECT_MAPPER.readValue(jsonString, SegmentPartitionMetadata.class);
  }

  /**
   * Returns the JSON equivalent of the object.
   *
   * @return JSON string equivalent of the object.
   * @throws IOException
   */
  public String toJsonString()
      throws IOException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  /**
   * Returns the number of partitions for the specified column.
   * Returns {@link #INVALID_NUM_PARTITIONS} if it does not exist for the column.
   *
   * @param column Column for which to get number of partitions.
   * @return Number of partitions of the column.
   */
  public int getNumPartitions(String column) {
    ColumnPartitionMetadata columnPartitionMetadata = _columnPartitionMap.get(column);
    return (columnPartitionMetadata != null) ? columnPartitionMetadata.getNumPartitions() : INVALID_NUM_PARTITIONS;
  }

  /**
   * Returns the list of partition ranges for the specified column.
   * Returns null if the column is not partitioned.
   *
   * @param column Column for which to return the list of partition ranges.
   * @return List of partition ranges for the specified column.
   */
  public List<IntRange> getPartitionRanges(String column) {
    ColumnPartitionMetadata columnPartitionMetadata = _columnPartitionMap.get(column);
    return (columnPartitionMetadata != null) ? columnPartitionMetadata.getPartitionRanges() : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SegmentPartitionMetadata that = (SegmentPartitionMetadata) o;
    return _columnPartitionMap.equals(that._columnPartitionMap);
  }

  @Override
  public int hashCode() {
    return _columnPartitionMap != null ? _columnPartitionMap.hashCode() : 0;
  }
}
