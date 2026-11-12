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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class StarTreeIndexConfig extends BaseJsonConfig {
  // Star-tree will be split with this order (time column is treated as dimension)
  private final List<String> _dimensionsSplitOrder;
  // Do not create star-node for these dimensions
  private final List<String> _skipStarNodeCreationForDimensions;
  // Function column pairs with delimiter "__", e.g. SUM__col1, MAX__col2, COUNT__*
  private final List<String> _functionColumnPairs;
  // Function column pairs config
  private final List<StarTreeAggregationConfig> _aggregationConfigs;
  // The upper bound of records to be scanned at the leaf node
  private final int _maxLeafRecords;
  // Whether the star-tree pre-aggregates with null-aware semantics
  private final boolean _nullHandlingEnabled;

  @JsonCreator
  public StarTreeIndexConfig(
      @JsonProperty(value = "dimensionsSplitOrder", required = true) List<String> dimensionsSplitOrder,
      @JsonProperty(value = "skipStarNodeCreationForDimensions") @Nullable
      List<String> skipStarNodeCreationForDimensions,
      @JsonProperty(value = "functionColumnPairs") @Nullable List<String> functionColumnPairs,
      @JsonProperty(value = "aggregationConfigs") @Nullable List<StarTreeAggregationConfig> aggregationConfigs,
      @JsonProperty(value = "maxLeafRecords") int maxLeafRecords,
      @JsonProperty(value = "nullHandlingEnabled") boolean nullHandlingEnabled) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(dimensionsSplitOrder),
        "'dimensionsSplitOrder' must be configured");
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions =
        CollectionUtils.isNotEmpty(skipStarNodeCreationForDimensions) ? skipStarNodeCreationForDimensions : null;
    _functionColumnPairs = CollectionUtils.isNotEmpty(functionColumnPairs) ? functionColumnPairs : null;
    _aggregationConfigs = CollectionUtils.isNotEmpty(aggregationConfigs) ? aggregationConfigs : null;
    _maxLeafRecords = maxLeafRecords;
    _nullHandlingEnabled = nullHandlingEnabled;
    Preconditions.checkArgument(_functionColumnPairs != null || _aggregationConfigs != null,
        "Either 'functionColumnPairs' or 'aggregationConfigs' must be configured");
  }

  /// Convenience constructor for a star-tree that is not null-aware, matching the behavior before
  /// `nullHandlingEnabled` was introduced.
  public StarTreeIndexConfig(List<String> dimensionsSplitOrder,
      @Nullable List<String> skipStarNodeCreationForDimensions, @Nullable List<String> functionColumnPairs,
      @Nullable List<StarTreeAggregationConfig> aggregationConfigs, int maxLeafRecords) {
    this(dimensionsSplitOrder, skipStarNodeCreationForDimensions, functionColumnPairs, aggregationConfigs,
        maxLeafRecords, false);
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  @Nullable
  public List<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  @Nullable
  public List<String> getFunctionColumnPairs() {
    return _functionColumnPairs;
  }

  @Nullable
  public List<StarTreeAggregationConfig> getAggregationConfigs() {
    return _aggregationConfigs;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  /// Returns whether this star-tree pre-aggregates with null-aware semantics.
  ///
  /// A null-aware star-tree keeps null dimension values in their own group instead of folding them into the column's
  /// default null value, and excludes null metric values from the pre-aggregation. It can therefore only serve queries
  /// with null handling enabled, while a regular star-tree can only serve queries with null handling disabled (or
  /// enabled queries over columns that happen to contain no nulls).
  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }
}
