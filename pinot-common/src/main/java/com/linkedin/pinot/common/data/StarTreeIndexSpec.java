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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class StarTreeIndexSpec {
  public static final Integer DEFAULT_MAX_LEAF_RECORDS = 100000; // TODO: determine a good number via experiment
  public static final int DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD = 10000;

  /** The upper bound on the number of leaf records to be scanned for any query */
  private Integer maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality) */
  private List<String> dimensionsSplitOrder;

  /** Dimensions for which to exclude star nodes at split. */
  private Set<String> skipStarNodeCreationForDimensions = Collections.emptySet();
  private Set<String> _skipMaterializationForDimensions;
  private int skipMaterializationCardinalityThreshold = DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;

  private boolean enableOffHeapFormat = true;

  public StarTreeIndexSpec() {}

  public Integer getMaxLeafRecords() {
    return maxLeafRecords;
  }

  public void setMaxLeafRecords(Integer maxLeafRecords) {
    this.maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    this.dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
    this.skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return skipStarNodeCreationForDimensions;
  }

  public Set<String> getskipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    _skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public int getskipMaterializationCardinalityThreshold() {
    return skipMaterializationCardinalityThreshold;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    this.skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeIndexSpec)) {
      return false;
    }
    StarTreeIndexSpec s = (StarTreeIndexSpec) o;
    return Objects.equal(maxLeafRecords, s.getMaxLeafRecords())
        && Objects.equal(dimensionsSplitOrder, s.getDimensionsSplitOrder());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(maxLeafRecords, dimensionsSplitOrder);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("maxLeafRecords", maxLeafRecords)
        .add("dimensionsSplitOrder", dimensionsSplitOrder)
        .toString();
  }

  /**
   * Returns True if StarTreeOffHeap is enabled, False otherwise.
   * @return
   */
  public boolean isEnableOffHeapFormat() {
    return enableOffHeapFormat;
  }

  /**
   * Enable/Disable StarTreeOffHeapFormat.
   * @param enableOffHeapFormat
   */
  public void setEnableOffHeapFormat(boolean enableOffHeapFormat) {
    this.enableOffHeapFormat = enableOffHeapFormat;
  }
}
