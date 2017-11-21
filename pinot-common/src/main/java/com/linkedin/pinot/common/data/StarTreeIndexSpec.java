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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class StarTreeIndexSpec {
  public static final Integer DEFAULT_MAX_LEAF_RECORDS = 100000; // TODO: determine a good number via experiment
  public static final int DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD = 10000;

  /** The upper bound on the number of leaf records to be scanned for any query */
  private Integer _maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality) */
  private List<String> _dimensionsSplitOrder;

  /** Dimensions for which to exclude star nodes at split. */
  private Set<String> _skipStarNodeCreationForDimensions;
  private Set<String> _skipMaterializationForDimensions;
  private int _skipMaterializationCardinalityThreshold = DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;

  private boolean _excludeSkipMaterializationDimensionsForStarTreeIndex;

  private static final String SEPARATOR = ",";
  private static final String DIMENSIONS_SPLIT_ORDER_PARAM = "dimensionsSplitOrder";
  private static final String MAX_LEAF_RECORDS_PARAM = "maxLeafRecords";
  private static final String SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD_PARAM = "skipMaterializationCardinalityThreshold";
  private static final String SKIP_MATERIALIZATION_FOR_DIMENSIONS_PARAM = "skipMaterializationForDimensions";
  private static final String SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS_PARAM = "skipStarNodeCreationForDimensions";

  public StarTreeIndexSpec() {
  }

  public StarTreeIndexSpec(String jsonString) {
    StarTreeIndexSpec starTreeIndexSpec = JSON.parseObject(jsonString, StarTreeIndexSpec.class);
    _dimensionsSplitOrder = starTreeIndexSpec.getDimensionsSplitOrder();
    _maxLeafRecords = starTreeIndexSpec.getMaxLeafRecords();
    _skipMaterializationCardinalityThreshold = starTreeIndexSpec.getSkipMaterializationCardinalityThreshold();
    _skipMaterializationForDimensions = starTreeIndexSpec.getSkipMaterializationForDimensions();
    _skipStarNodeCreationForDimensions = starTreeIndexSpec.getSkipStarNodeCreationForDimensions();
  }

  public Integer getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public void setMaxLeafRecords(Integer maxLeafRecords) {
    this._maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public Set<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    _skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public int getSkipMaterializationCardinalityThreshold() {
    return _skipMaterializationCardinalityThreshold;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    _skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  public boolean isExcludeSkipMaterializationDimensionsForStarTreeIndex() {
    return _excludeSkipMaterializationDimensionsForStarTreeIndex;
  }

  public void setExcludeSkipMaterializationDimensionsForStarTreeIndex(
      boolean excludeSkipMaterializationDimensionsForStarTreeIndex) {
    _excludeSkipMaterializationDimensionsForStarTreeIndex = excludeSkipMaterializationDimensionsForStarTreeIndex;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public String toJsonString() {
    Map<String, Object> map = new HashMap<>();
    map.put(DIMENSIONS_SPLIT_ORDER_PARAM, _dimensionsSplitOrder);
    map.put(MAX_LEAF_RECORDS_PARAM, _maxLeafRecords);
    map.put(SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD_PARAM, _skipMaterializationCardinalityThreshold);
    map.put(SKIP_MATERIALIZATION_FOR_DIMENSIONS_PARAM, _skipMaterializationForDimensions);
    map.put(SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS_PARAM, _skipStarNodeCreationForDimensions);
    JSONObject jsonObject = new JSONObject(map);
    return jsonObject.toString();
  }
}
