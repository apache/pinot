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
package com.linkedin.pinot.core.startree.v2.builder;

import com.linkedin.pinot.common.config.StarTreeIndexConfig;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


/**
 * The {@code StarTreeV2BuilderConfig} class contains the configuration for star-tree builder.
 */
public class StarTreeV2BuilderConfig {
  public static final int DEFAULT_MAX_LEAF_RECORDS = 10_000;

  private final List<String> _dimensionsSplitOrder;
  private final Set<String> _skipStarNodeCreationForDimensions;
  private final Set<AggregationFunctionColumnPair> _functionColumnPairs;
  private final int _maxLeafRecords;

  public static StarTreeV2BuilderConfig fromIndexConfig(StarTreeIndexConfig indexConfig) {
    Builder builder = new Builder();
    builder.setDimensionsSplitOrder(indexConfig.getDimensionsSplitOrder());
    List<String> skipStarNodeCreationForDimensions = indexConfig.getSkipStarNodeCreationForDimensions();
    if (skipStarNodeCreationForDimensions != null && !skipStarNodeCreationForDimensions.isEmpty()) {
      builder.setSkipStarNodeCreationForDimensions(new HashSet<>(skipStarNodeCreationForDimensions));
    }
    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    for (String functionColumnPair : indexConfig.getFunctionColumnPairs()) {
      functionColumnPairs.add(AggregationFunctionColumnPair.fromColumnName(functionColumnPair));
    }
    builder.setFunctionColumnPairs(functionColumnPairs);
    int maxLeafRecords = indexConfig.getMaxLeafRecords();
    if (maxLeafRecords > 0) {
      builder.setMaxLeafRecords(maxLeafRecords);
    }
    return builder.build();
  }

  private StarTreeV2BuilderConfig(List<String> dimensionsSplitOrder, Set<String> skipStarNodeCreationForDimensions,
      Set<AggregationFunctionColumnPair> functionColumnPairs, int maxLeafRecords) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
    _functionColumnPairs = functionColumnPairs;
    _maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairs() {
    return _functionColumnPairs;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("splitOrder", _dimensionsSplitOrder)
        .append("skipStarNodeCreation", _skipStarNodeCreationForDimensions)
        .append("functionColumnPairs", _functionColumnPairs)
        .append("maxLeafRecords", _maxLeafRecords)
        .toString();
  }

  public static class Builder {
    private List<String> _dimensionsSplitOrder;
    private Set<String> _skipStarNodeCreationForDimensions;
    private Set<AggregationFunctionColumnPair> _functionColumnPairs;
    private int _maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

    public Builder setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
      _dimensionsSplitOrder = dimensionsSplitOrder;
      return this;
    }

    public Builder setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
      _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
      return this;
    }

    public Builder setFunctionColumnPairs(Set<AggregationFunctionColumnPair> functionColumnPairs) {
      _functionColumnPairs = functionColumnPairs;
      return this;
    }

    public Builder setMaxLeafRecords(int maxLeafRecords) {
      _maxLeafRecords = maxLeafRecords;
      return this;
    }

    public StarTreeV2BuilderConfig build() {
      if (_dimensionsSplitOrder == null || _dimensionsSplitOrder.isEmpty()) {
        throw new IllegalStateException("Illegal dimensions split order: " + _dimensionsSplitOrder);
      }
      if (_skipStarNodeCreationForDimensions == null) {
        _skipStarNodeCreationForDimensions = Collections.emptySet();
      }
      if (!_dimensionsSplitOrder.containsAll(_skipStarNodeCreationForDimensions)) {
        throw new IllegalStateException(
            "Can not skip star-node creation for dimension not in the split order, dimensionsSplitOrder: "
                + _dimensionsSplitOrder + ", skipStarNodeCreationForDimensions: " + _skipStarNodeCreationForDimensions);
      }
      if (_functionColumnPairs == null || _functionColumnPairs.isEmpty()) {
        throw new IllegalStateException("Illegal function-column pairs: " + _functionColumnPairs);
      }
      if (_maxLeafRecords <= 0) {
        throw new IllegalStateException("Illegal maximum number of leaf records: " + _maxLeafRecords);
      }
      return new StarTreeV2BuilderConfig(_dimensionsSplitOrder, _skipStarNodeCreationForDimensions,
          _functionColumnPairs, _maxLeafRecords);
    }
  }
}
