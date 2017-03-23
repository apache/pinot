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
package com.linkedin.pinot.core.indexsegment.generator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


@SuppressWarnings("unused") // Suppress incorrect warning, as methods are used for json ser/de.
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentPartitionConfig {
  private final Map<String, ColumnPartitionConfig> _columnPartitionMap;

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
  public PartitionFunction getPartitionFunction(@Nonnull String column) {
    ColumnPartitionConfig columnPartitionConfig = _columnPartitionMap.get(column);
    return (columnPartitionConfig != null) ? columnPartitionConfig.getPartitionFunction() : null;
  }

  /**
   * Returns the list of valid ranges for the given columns, null if ranges not defined for the column.
   *
   * @param column Column for which to return the ranges.
   * @return List of ranges for the column.
   */
  @Nullable
  public List<IntRange> getPartitionRanges(@Nonnull String column) {
    ColumnPartitionConfig columnPartitionConfig = _columnPartitionMap.get(column);
    if (columnPartitionConfig != null) {
      return columnPartitionConfig.getPartitionRanges();
    }
    return null;
  }
}
