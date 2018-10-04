/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.configuration.Configuration;

import static com.linkedin.pinot.core.startree.v2.StarTreeV2Constants.MetadataKey.*;


/**
 * The {@code StarTreeV2Metadata} contains the metadata for a single star-tree.
 */
public class StarTreeV2Metadata {
  private final int _numDocs;
  private final List<String> _dimensionsSplitOrder;
  private final Set<AggregationFunctionColumnPair> _functionColumnPairs;

  // The following properties are useful for generating the builder config
  private final int _maxLeafRecords;
  private final Set<String> _skipStarNodeCreationForDimensions;

  public StarTreeV2Metadata(int numDocs, List<String> dimensionsSplitOrder,
      Set<AggregationFunctionColumnPair> functionColumnPairs, int maxLeafRecords,
      Set<String> skipStarNodeCreationForDimensions) {
    _numDocs = numDocs;
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _functionColumnPairs = functionColumnPairs;
    _maxLeafRecords = maxLeafRecords;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  @SuppressWarnings("unchecked")
  public StarTreeV2Metadata(Configuration metadataProperties) {
    _numDocs = metadataProperties.getInt(TOTAL_DOCS);
    _dimensionsSplitOrder = metadataProperties.getList(DIMENSIONS_SPLIT_ORDER);
    _functionColumnPairs = new HashSet<>();
    for (Object functionColumnPair : metadataProperties.getList(FUNCTION_COLUMN_PAIRS)) {
      _functionColumnPairs.add(AggregationFunctionColumnPair.fromColumnName((String) functionColumnPair));
    }
    _maxLeafRecords = metadataProperties.getInt(MAX_LEAF_RECORDS);
    _skipStarNodeCreationForDimensions =
        new HashSet<>(metadataProperties.getList(SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS));
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

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }
}
