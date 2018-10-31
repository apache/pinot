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
package com.linkedin.pinot.common.segment;

import java.util.List;


/**
 * This class holds the star tree specific metadata for a segment.
 */
public class StarTreeMetadata {
  private int _maxLeafRecords;
  private List<String> _dimensionsSplitOrder;
  private List<String> _skipStarNodeCreationForDimensions;
  private List<String> _skipMaterializationForDimensions;
  private int _skipMaterializationCardinality;

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public void setMaxLeafRecords(int maxLeafRecords) {
    _maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public List<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public void setSkipStarNodeCreationForDimensions(List<String> skipStarNodeCreationForDimensions) {
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public List<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  public void setSkipMaterializationForDimensions(List<String> skipMaterializationForDimensions) {
    _skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public int getSkipMaterializationCardinality() {
    return _skipMaterializationCardinality;
  }

  public void setSkipMaterializationCardinality(int skipMaterializationCardinality) {
    _skipMaterializationCardinality = skipMaterializationCardinality;
  }
}
