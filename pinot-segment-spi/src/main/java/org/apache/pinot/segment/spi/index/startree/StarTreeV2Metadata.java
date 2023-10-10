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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;


/**
 * The {@code StarTreeV2Metadata} contains the metadata for a single star-tree.
 */
public class StarTreeV2Metadata {
  private final int _numDocs;
  private final List<String> _dimensionsSplitOrder;
  private final Set<AggregationFunctionColumnPair> _functionColumnPairs;

  private final Set<AggregationFunctionColumnPair> _functionColumnPairsConfig;

  // The following properties are useful for generating the builder config
  private final int _maxLeafRecords;
  private final Set<String> _skipStarNodeCreationForDimensions;

  public StarTreeV2Metadata(Configuration metadataProperties) {
    _numDocs = metadataProperties.getInt(MetadataKey.TOTAL_DOCS);
    _dimensionsSplitOrder = Arrays.asList(metadataProperties.getStringArray(MetadataKey.DIMENSIONS_SPLIT_ORDER));
    _functionColumnPairs = new HashSet<>();
    _functionColumnPairsConfig = new HashSet<>();
    for (String functionColumnPair : metadataProperties.getStringArray(MetadataKey.FUNCTION_COLUMN_PAIRS)) {
      _functionColumnPairs.add(AggregationFunctionColumnPair.fromColumnName(functionColumnPair));
      Configuration functionColPairsConfig =
          metadataProperties.subset(MetadataKey.FUNCTION_COLUMN_PAIRS_CONFIG + "." + functionColumnPair);
      if (!functionColPairsConfig.isEmpty()) {
        _functionColumnPairsConfig.add(AggregationFunctionColumnPair.fromConfiguration(functionColPairsConfig));
      }
    } _maxLeafRecords = metadataProperties.getInt(MetadataKey.MAX_LEAF_RECORDS);
    _skipStarNodeCreationForDimensions = new HashSet<>(
        Arrays.asList(metadataProperties.getStringArray(MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS)));
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairs() {
    return _functionColumnPairs;
  }

  public boolean containsFunctionColumnPair(AggregationFunctionColumnPair functionColumnPair) {
    return _functionColumnPairs.contains(functionColumnPair);
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairsConfig() {
    return _functionColumnPairsConfig;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }
}
