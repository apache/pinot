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
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.JsonUtils;


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

  @JsonCreator
  public StarTreeIndexConfig(
      @JsonProperty(value = "dimensionsSplitOrder", required = true) List<String> dimensionsSplitOrder,
      @JsonProperty(value = "skipStarNodeCreationForDimensions") @Nullable
      List<String> skipStarNodeCreationForDimensions,
      @JsonProperty(value = "functionColumnPairs") @Nullable List<String> functionColumnPairs,
      @JsonProperty(value = "aggregationConfigs") @Nullable List<StarTreeAggregationConfig> aggregationConfigs,
      @JsonProperty(value = "maxLeafRecords") int maxLeafRecords) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(dimensionsSplitOrder),
        "'dimensionsSplitOrder' must be configured");
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions =
        CollectionUtils.isNotEmpty(skipStarNodeCreationForDimensions) ? skipStarNodeCreationForDimensions : null;
    _functionColumnPairs = CollectionUtils.isNotEmpty(functionColumnPairs) ? functionColumnPairs : null;
    _aggregationConfigs = CollectionUtils.isNotEmpty(aggregationConfigs) ? aggregationConfigs : null;
    _maxLeafRecords = maxLeafRecords;
    Preconditions.checkArgument(_functionColumnPairs != null || _aggregationConfigs != null,
        "Either 'functionColumnPairs' or 'aggregationConfigs' must be configured");
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

  /**
   * Curated slim serializer. Standalone (not inheriting from {@link IndexConfig}) — this class
   * extends {@link BaseJsonConfig} directly and has no {@code disabled} field. The constructor
   * coerces empty optional lists to {@code null}, so the optional list fields are emitted only
   * when non-null. {@code maxLeafRecords} is a primitive {@code int}; "absent" and {@code 0}
   * collapse, so {@code 0} is treated as the default and omitted.
   */
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = JsonUtils.newObjectNode();
    node.set("dimensionsSplitOrder", JsonUtils.objectToJsonNode(_dimensionsSplitOrder));
    if (_skipStarNodeCreationForDimensions != null) {
      node.set("skipStarNodeCreationForDimensions", JsonUtils.objectToJsonNode(_skipStarNodeCreationForDimensions));
    }
    if (_functionColumnPairs != null) {
      node.set("functionColumnPairs", JsonUtils.objectToJsonNode(_functionColumnPairs));
    }
    if (_aggregationConfigs != null) {
      node.set("aggregationConfigs", JsonUtils.objectToJsonNode(_aggregationConfigs));
    }
    if (_maxLeafRecords != 0) {
      node.put("maxLeafRecords", _maxLeafRecords);
    }
    return node;
  }
}
