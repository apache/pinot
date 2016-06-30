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

  private Schema _schema;
  private File _outDir;
  private List<String> _dimensionsSplitOrder;
  private Set<String> _skipStarNodeCreationForDimensions;
  private Set<String> _skipMaterializationFroDimensions;
  private int _maxLeafRecords = StarTreeIndexSpec.DEFAULT_MAX_LEAF_RECORDS;
  private int _skipMaterializationCardinalityThreshold =
      StarTreeIndexSpec.DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;
  private int _skipSplitOnTimeColumnThreshold = StarTreeIndexSpec.DEFAULT_SKIP_SPLIT_ON_TIME_COLUMN_THRESHOLD;

  public StarTreeBuilderConfig() {
  }

  public void setSchema(Schema schema) {
    _schema = schema;
  }

  public Schema getSchema() {
    return _schema;
  }

  public void setOutDir(File outDir) {
    _outDir = outDir;
  }

  public File getOutDir() {
    return _outDir;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    _skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  public int getSkipMaterializationCardinalityThreshold() {
    return _skipMaterializationCardinalityThreshold;
  }

  public void setMaxLeafRecords(int maxLeafRecords) {
    _maxLeafRecords = maxLeafRecords;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> excludedStarDimensions) {
    _skipStarNodeCreationForDimensions = excludedStarDimensions;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    _skipMaterializationFroDimensions = skipMaterializationForDimensions;
  }

  public Set<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationFroDimensions;
  }

  public void setSkipSplitOnTimeColumnThreshold(int skipSplitOnTimeColumnThreshold) {
    _skipSplitOnTimeColumnThreshold = skipSplitOnTimeColumnThreshold;
  }

  public int getSkipSplitOnTimeColumnThreshold() {
    return _skipSplitOnTimeColumnThreshold;
  }
}
