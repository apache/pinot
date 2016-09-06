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
import java.util.Map;


/**
 * This class holds the star tree specific metadata for a segment.
 */
public class StarTreeMetadata {

  private List<String> _dimensionsSplitOrder;
  private List<String> _skipStarNodeCreationForDimensions;
  private List<String> _skipMaterializationForDimensions;

  private long _maxLeafRecords;
  private long _skipMaterializationCardinality;

  private boolean _enableHll;
  private int _hllLog2m;
  private Map<String, String> _hllOriginToDerivedColumnMap;

  public StarTreeMetadata() {
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public long getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public void setMaxLeafRecords(Long maxLeafRecords) {
    _maxLeafRecords = maxLeafRecords;
  }

  public List<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public void setSkipStarNodeCreationForDimensions(List<String> skipStarNodeCreationForDimensions) {
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public long getSkipMaterializationCardinality() {
    return _skipMaterializationCardinality;
  }

  public void setSkipMaterializationCardinality(Long skipMaterializationCardinality) {
    _skipMaterializationCardinality = skipMaterializationCardinality;
  }

  public List<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  public void setSkipMaterializationForDimensions(List<String> skipMaterializationForDimensions) {
    _skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public boolean isEnableHll() {
    return _enableHll;
  }

  public void setEnableHll(boolean enableHll) {
    _enableHll = enableHll;
  }

  public int getHllLog2m() {
    return _hllLog2m;
  }

  public void setHllLog2m(int hllLog2m) {
    _hllLog2m = hllLog2m;
  }

  public String getDerivedHllColumnFromOrigin(String originColumn) {
    String ret = _hllOriginToDerivedColumnMap.get(originColumn);
    if (ret == null) {
      throw new IllegalArgumentException("Hll derived column does not exist for " + originColumn);
    }
    return ret;
  }

  public void setHllOriginToDerivedColumnMap(Map<String, String> hllOriginToDerivedColumnMap) {
    _hllOriginToDerivedColumnMap = hllOriginToDerivedColumnMap;
  }
}
