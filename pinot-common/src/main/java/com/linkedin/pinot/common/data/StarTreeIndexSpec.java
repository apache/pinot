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
package com.linkedin.pinot.common.data;

import com.google.common.base.Objects;
import java.util.List;
import java.util.Set;

public class StarTreeIndexSpec {
  public static final int DEFAULT_MAX_LEAF_RECORDS = 100000; // TODO: determine a good number via experiment
  public static final int DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD = 10000;
  public static final int DEFAULT_SKIP_SPLIT_ON_TIME_COLUMN_THRESHOLD = 1000;

  /** The upper bound on the number of leaf records to be scanned for any query. */
  private int _maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** The threshold to decide whether to skip or split on columns if not explicitly specified. */
  private int _skipMaterializationCardinalityThreshold = DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;

  /** The threshold to decide whether to further split on time column for leaf nodes. */
  private int _skipSplitOnTimeColumnThreshold = DEFAULT_SKIP_SPLIT_ON_TIME_COLUMN_THRESHOLD;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality). */
  private List<String> _dimensionsSplitOrder;

  /** Dimensions for which to exclude star nodes at split. */
  private Set<String> _skipStarNodeCreationForDimensions;

  /** Dimensions for which to skip from materialization, and cannot be queried on. */
  private Set<String> _skipMaterializationForDimensions;

  public void setMaxLeafRecords(int maxLeafRecords) {
    this._maxLeafRecords = maxLeafRecords;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    _skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  public int getSkipMaterializationCardinalityThreshold() {
    return _skipMaterializationCardinalityThreshold;
  }

  public void setSkipSplitOnTimeColumnThreshold(int skipSplitOnTimeColumnThreshold) {
    _skipSplitOnTimeColumnThreshold = skipSplitOnTimeColumnThreshold;
  }

  public int getSkipSplitOnTimeColumnThreshold() {
    return _skipSplitOnTimeColumnThreshold;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    _skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public Set<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeIndexSpec)) {
      return false;
    }
    StarTreeIndexSpec s = (StarTreeIndexSpec) o;
    return (_maxLeafRecords == s.getMaxLeafRecords())
        && (_skipMaterializationCardinalityThreshold == s.getSkipMaterializationCardinalityThreshold())
        && (_skipSplitOnTimeColumnThreshold == s.getSkipSplitOnTimeColumnThreshold())
        && (_dimensionsSplitOrder.equals(s.getDimensionsSplitOrder()))
        && (_skipStarNodeCreationForDimensions.equals(s.getSkipStarNodeCreationForDimensions()))
        && (_skipMaterializationForDimensions.equals(s.getSkipMaterializationForDimensions()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_maxLeafRecords, _skipMaterializationCardinalityThreshold, _skipSplitOnTimeColumnThreshold,
        _dimensionsSplitOrder, _skipStarNodeCreationForDimensions, _skipMaterializationForDimensions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("maxLeafRecords", _maxLeafRecords)
        .add("skipMaterializationCardinalityThreshold", _skipMaterializationCardinalityThreshold)
        .add("skipSplitOnTimeColumnThreshold", _skipSplitOnTimeColumnThreshold)
        .add("dimensionsSplitOrder", _dimensionsSplitOrder)
        .add("skipStarNodeCreationForDimensions", _skipStarNodeCreationForDimensions)
        .add("skipMaterializationForDimensions", _skipMaterializationForDimensions)
        .toString();
  }
}
