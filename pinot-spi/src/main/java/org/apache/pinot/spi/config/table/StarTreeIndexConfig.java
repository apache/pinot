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
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class StarTreeIndexConfig extends BaseJsonConfig {
  // Star-tree will be split with this order (time column is treated as dimension)
  private final List<String> _dimensionsSplitOrder;
  // Do not create star-node for these dimensions
  private final List<String> _skipStarNodeCreationForDimensions;
  // Function column pairs with delimiter "__", e.g. SUM__col1, MAX__col2, COUNT__*
  private final List<String> _functionColumnPairs;
  // Compression mapping for function column pairs
  private final Properties _functionColumnPairsConfig;
  // The upper bound of records to be scanned at the leaf node
  private final int _maxLeafRecords;

  @JsonCreator
  public StarTreeIndexConfig(
      @JsonProperty(value = "dimensionsSplitOrder", required = true) List<String> dimensionsSplitOrder,
      @JsonProperty("skipStarNodeCreationForDimensions") @Nullable List<String> skipStarNodeCreationForDimensions,
      @JsonProperty(value = "functionColumnPairs", required = true) List<String> functionColumnPairs,
      @JsonProperty(value = "functionColumnPairsConfig") @Nullable Properties functionColumnPairsConfig,
      @JsonProperty("maxLeafRecords") int maxLeafRecords) {
    Preconditions
        .checkArgument(CollectionUtils.isNotEmpty(dimensionsSplitOrder), "'dimensionsSplitOrder' must be configured");
    Preconditions
        .checkArgument(CollectionUtils.isNotEmpty(functionColumnPairs), "'functionColumnPairs' must be configured");
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
    _functionColumnPairs = functionColumnPairs;
    _maxLeafRecords = maxLeafRecords;
    _functionColumnPairsConfig = functionColumnPairsConfig;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  @Nullable
  public List<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public List<String> getFunctionColumnPairs() {
    return _functionColumnPairs;
  }
  @Nullable
  public Properties getFunctionColumnPairsConfig() {
    return _functionColumnPairsConfig;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }
}
