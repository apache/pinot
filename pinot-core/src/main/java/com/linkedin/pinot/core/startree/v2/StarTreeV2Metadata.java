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

import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class StarTreeV2Metadata {
  private final int _numDocs;
  private final List<String> _dimensionsSplitOrder;
  private final Set<AggregationFunctionColumnPair> _aggregationFunctionColumnPairs;

  // The following properties are useful for generating the builder config
  private final int _maxNumLeafRecords;
  private final Set<String> _skipStarNodeCreationForDimensions;

  public StarTreeV2Metadata(int numDocs, @Nonnull List<String> dimensionsSplitOrder,
      @Nonnull Set<AggregationFunctionColumnPair> aggregationFunctionColumnPairs, int maxNumLeafRecords,
      @Nonnull Set<String> skipStarNodeCreationForDimensions) {
    _numDocs = numDocs;
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _aggregationFunctionColumnPairs = aggregationFunctionColumnPairs;
    _maxNumLeafRecords = maxNumLeafRecords;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public Set<AggregationFunctionColumnPair> getAggregationFunctionColumnPairs() {
    return _aggregationFunctionColumnPairs;
  }

  public boolean containsAggregationFunctionColumnPair(AggregationFunctionColumnPair pair) {
    return _aggregationFunctionColumnPairs.contains(pair);
  }

  public int getMaxNumLeafRecords() {
    return _maxNumLeafRecords;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }
}
