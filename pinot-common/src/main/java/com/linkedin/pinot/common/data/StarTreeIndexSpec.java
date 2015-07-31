/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

public class StarTreeIndexSpec {
  private static final Integer DEFAULT_MAX_LEAF_RECORDS = 10000; // TODO: determine a good number via experiment

  /** The upper bound on the number of leaf records to be scanned for any query */
  private Integer maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality) */
  private List<String> splitOrder;

  /** The dimensions that should be included in the tree, but never split on (e.g. time) */
  private List<String> splitExcludes;

  /** The dimensions that should not be included in the tree (i.e. always interpret at aggregate level) */
  private List<String> excludedDimensions = Collections.emptyList();

  public StarTreeIndexSpec() {}

  public Integer getMaxLeafRecords() {
    return maxLeafRecords;
  }

  public void setMaxLeafRecords(Integer maxLeafRecords) {
    this.maxLeafRecords = maxLeafRecords;
  }

  public List<String> getSplitOrder() {
    return splitOrder;
  }

  public void setSplitOrder(List<String> splitOrder) {
    this.splitOrder = splitOrder;
  }

  public List<String> getSplitExcludes() {
    return splitExcludes;
  }

  public void setSplitExcludes(List<String> splitExcludes) {
    this.splitExcludes = splitExcludes;
  }

  public List<String> getExcludedDimensions() {
    return excludedDimensions;
  }

  public void setExcludedDimensions(List<String> excludedDimensions) {
    this.excludedDimensions = excludedDimensions;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeIndexSpec)) {
      return false;
    }
    StarTreeIndexSpec s = (StarTreeIndexSpec) o;
    return Objects.equal(maxLeafRecords, s.getMaxLeafRecords())
        && Objects.equal(splitExcludes, s.getSplitExcludes())
        && Objects.equal(excludedDimensions, s.getExcludedDimensions())
        && Objects.equal(splitOrder, s.getSplitOrder());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(maxLeafRecords, splitOrder, splitExcludes, excludedDimensions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("maxLeafRecords", maxLeafRecords)
        .add("splitExcludes", splitExcludes)
        .add("excludedDimensions", excludedDimensions)
        .add("splitOrder", splitOrder)
        .toString();
  }
}
