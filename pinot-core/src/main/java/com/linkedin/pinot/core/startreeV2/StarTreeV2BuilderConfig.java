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

package com.linkedin.pinot.core.startreeV2;

import com.google.common.collect.ListMultimap;
import java.io.File;
import java.util.Set;
import java.util.List;
import com.google.common.collect.ArrayListMultimap;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;


public class StarTreeV2BuilderConfig {

  private File _outDir;
  private int _maxNumLeafRecords;
  private List<String> _dimensionsSplitOrder;
  private List<DimensionFieldSpec> _dimensions;
  private Set<String> _dimensionsWithoutStarNode;
  private ListMultimap<MetricFieldSpec, String> _metric2aggFuncPairs;

  /**
   * Set the temporary directory for star tree.
   */
  public void setOutDir(File outDir) {
    _outDir = outDir;
  }

  /**
   * get the temporary directory for star tree.
   */
  public File getOutDir() {
    return _outDir;
  }

  /**
   * Set the limit for the maximum number of records which a leaf node can have.
   * If the number of records in a node exceed this value, it will split further down.
   */
  public void setMaxNumLeafRecords(int maxNumLeafRecords) {
    _maxNumLeafRecords = maxNumLeafRecords;
  }

  /**
   * Get the limit for the maximum number of records which a leaf node can have.
   */
  public int getMaxNumLeafRecords() {
    return _maxNumLeafRecords;
  }

  /**
   * Set the dimensions present in this star tree.
   */
  public void setDimensions(List<DimensionFieldSpec> dimensions) {
    _dimensions = dimensions;
  }

  /**
   * get the dimensions present in this star tree.
   */
  public List<DimensionFieldSpec> getDimensions() {
    return _dimensions;
  }

  /**
   * Set the split order of the dimensions.
   */
  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  /**
   * Get the split order of the dimensions.
   */
  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  /**
   * Set the dimensions for which there is no need to create star nodes.
   */
  public void setDimensionsWithoutStarNode(Set<String> excludedStarDimensions) {
    _dimensionsWithoutStarNode = excludedStarDimensions;
  }

  /**
   * Get the dimensions for which there is no need to create star nodes.
   */
  public Set<String> getDimensionsWithoutStarNode() {
    return _dimensionsWithoutStarNode;
  }

  /**
   * Set the mapping of metric to aggregation function
   */
  public void setMetric2aggFuncPairs(ListMultimap<MetricFieldSpec, String> metric2aggFuncPairs) {
    _metric2aggFuncPairs = metric2aggFuncPairs;
  }

  /**
   * Get the mapping of metric to aggregation function.
   */
  public ListMultimap<MetricFieldSpec, String> getMetric2aggFuncPairs() {
    return _metric2aggFuncPairs;
  }
}
