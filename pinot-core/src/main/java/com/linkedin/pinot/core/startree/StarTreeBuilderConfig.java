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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import java.io.File;
import java.util.List;
import java.util.Set;


public class StarTreeBuilderConfig {

  public Schema schema;

  public List<String> dimensionsSplitOrder;

  public int maxLeafRecords;

  File outDir;

  private Set<String> skipStarNodeCreationForDimensions;
  private Set<String> skipMaterializationForDimensions;
  private int skipMaterializationCardinalityThreshold =
      StarTreeIndexSpec.DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;
  private boolean _excludeSkipMaterializationDimensionsForStarTreeIndex;
  private boolean _enableOffHeapFormat;

  public StarTreeBuilderConfig() {
  }

  public File getOutDir() {
    return outDir;
  }

  public void setOutDir(File outDir) {
    this.outDir = outDir;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public List<String> getDimensionsSplitOrder() {
    return dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    this.dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public int getMaxLeafRecords() {
    return maxLeafRecords;
  }

  public void setMaxLeafRecords(int maxLeafRecords) {
    this.maxLeafRecords = maxLeafRecords;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> excludedStarDimensions) {
    this.skipStarNodeCreationForDimensions = excludedStarDimensions;
  }

  public int getSkipMaterializationCardinalityThreshold() {
    return skipMaterializationCardinalityThreshold;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    this.skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  /**
   * Get of dimension names for which not to create star nodes at split.
   */
  public Set<String> getSkipStarNodeCreationForDimensions() {
    return skipStarNodeCreationForDimensions;
  }

  /**
   * Get the set of dimension names that should be skipped from materialization.
   * @return
   */
  public Set<String> getSkipMaterializationForDimensions() {
    return skipMaterializationForDimensions;
  }

  /**
   * Set the set of dimensions for which to skip materialization
   * @param skipMaterializationForDimensions
   */
  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    this.skipMaterializationForDimensions = skipMaterializationForDimensions;
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

  /**
   * Returns True if StarTreeOffHeap is enabled, false otherwise.
   * @return
   */
  public boolean isEnableOffHeapFormat() {
    return _enableOffHeapFormat;
  }

  /**
   * Enable/Disable StarTreeOffHeap
   * @param enableOffHeapFormat
   */
  public void setEnableOffHeapFormat(boolean enableOffHeapFormat) {
    _enableOffHeapFormat = enableOffHeapFormat;
  }
}
