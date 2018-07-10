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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.Schema;
import java.io.File;
import java.util.List;
import java.util.Set;


public class StarTreeBuilderConfig {
  private File _outDir;
  private Schema _schema;
  private List<String> _dimensionsSplitOrder;
  private Set<String> _skipStarNodeCreationDimensions;
  private Set<String> _skipMaterializationDimensions;
  private int _maxNumLeafRecords;
  private int _skipMaterializationCardinalityThreshold;
  private boolean _excludeSkipMaterializationDimensionsForStarTreeIndex;

  public StarTreeBuilderConfig() {
  }

  public File getOutDir() {
    return _outDir;
  }

  public void setOutDir(File outDir) {
    _outDir = outDir;
  }

  public Schema getSchema() {
    return _schema;
  }

  public void setSchema(Schema schema) {
    _schema = schema;
  }

  /**
   * Get the split order of the dimensions.
   */
  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  /**
   * Get of dimensions for which not to create star nodes at split.
   */
  public Set<String> getSkipStarNodeCreationDimensions() {
    return _skipStarNodeCreationDimensions;
  }

  public void setSkipStarNodeCreationDimensions(Set<String> excludedStarDimensions) {
    _skipStarNodeCreationDimensions = excludedStarDimensions;
  }

  /**
   * Get the dimensions that should be skipped from materialization.
   */
  public Set<String> getSkipMaterializationDimensions() {
    return _skipMaterializationDimensions;
  }

  public void setSkipMaterializationDimensions(Set<String> skipMaterializationDimensions) {
    _skipMaterializationDimensions = skipMaterializationDimensions;
  }

  /**
   * Get the threshold of the cardinality to determine whether to skip dimensions from materialization. This value will
   * be used when skip materialization dimensions are not specified.
   */
  public int getSkipMaterializationCardinalityThreshold() {
    return _skipMaterializationCardinalityThreshold;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    _skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  /**
   * Get the maximum number of records (documents) in a leaf node. If the number of records inside a node exceeds this
   * value, the node will be split.
   */
  public int getMaxNumLeafRecords() {
    return _maxNumLeafRecords;
  }

  public void setMaxNumLeafRecords(int maxNumLeafRecords) {
    _maxNumLeafRecords = maxNumLeafRecords;
  }

  /**
   * Whether to remove dimensions that are skipped materialization and aggregate the data before generating the star
   * tree.
   */
  public boolean isExcludeSkipMaterializationDimensionsForStarTreeIndex() {
    return _excludeSkipMaterializationDimensionsForStarTreeIndex;
  }

  public void setExcludeSkipMaterializationDimensionsForStarTreeIndex(
      boolean excludeSkipMaterializationDimensionsForStarTreeIndex) {
    _excludeSkipMaterializationDimensionsForStarTreeIndex = excludeSkipMaterializationDimensionsForStarTreeIndex;
  }
}
