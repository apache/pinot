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

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class StarTreeIndexSpec {
  public static final int DEFAULT_MAX_LEAF_RECORDS = 100000; // TODO: determine a good number via experiment
  public static final int DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD = 10000;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** The upper bound on the number of leaf records to be scanned for any query */
  private int _maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality) */
  private List<String> _dimensionsSplitOrder;

  /** Dimensions for which to exclude star nodes at split. */
  private Set<String> _skipStarNodeCreationForDimensions;
  private Set<String> _skipMaterializationForDimensions;
  private int _skipMaterializationCardinalityThreshold = DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;

  private boolean _excludeSkipMaterializationDimensionsForStarTreeIndex;

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

  public String toJsonString() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  /**
   * Builds and returns StarTreeIndexSpec from specified file.
   *
   * @param starTreeIndexSpecFile File containing star tree index spec.
   * @return StarTreeIndexSpec object de-serialized from the file.
   * @throws IOException
   */
  public static StarTreeIndexSpec fromFile(File starTreeIndexSpecFile) throws IOException {
    return OBJECT_MAPPER.readValue(starTreeIndexSpecFile, StarTreeIndexSpec.class);
  }

  public static StarTreeIndexSpec fromJsonString(String jsonString) throws IOException {
    return OBJECT_MAPPER.readValue(jsonString, StarTreeIndexSpec.class);
  }

  public static StarTreeIndexSpec fromStarTreeMetadata(StarTreeMetadata starTreeMetadata) {
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
    starTreeIndexSpec.setMaxLeafRecords(starTreeMetadata.getMaxLeafRecords());
    starTreeIndexSpec.setDimensionsSplitOrder(starTreeMetadata.getDimensionsSplitOrder());
    starTreeIndexSpec.setSkipStarNodeCreationForDimensions(
        Sets.newHashSet(starTreeMetadata.getSkipStarNodeCreationForDimensions()));
    starTreeIndexSpec.setSkipMaterializationForDimensions(
        Sets.newHashSet(starTreeMetadata.getSkipMaterializationForDimensions()));
    starTreeIndexSpec.setSkipMaterializationCardinalityThreshold(starTreeMetadata.getSkipMaterializationCardinality());
    return starTreeIndexSpec;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    StarTreeIndexSpec that = (StarTreeIndexSpec) o;

    return EqualityUtils.isEqual(_maxLeafRecords, that._maxLeafRecords) &&
        EqualityUtils.isEqual( _skipMaterializationCardinalityThreshold, that._skipMaterializationCardinalityThreshold) &&
        EqualityUtils.isEqual(_excludeSkipMaterializationDimensionsForStarTreeIndex, that._excludeSkipMaterializationDimensionsForStarTreeIndex) &&
        EqualityUtils.isEqual(_dimensionsSplitOrder, that._dimensionsSplitOrder) &&
        EqualityUtils.isEqual(_skipStarNodeCreationForDimensions, that._skipStarNodeCreationForDimensions) &&
        EqualityUtils.isEqual(_skipMaterializationForDimensions, that._skipMaterializationForDimensions);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_maxLeafRecords);
    result = EqualityUtils.hashCodeOf(result, _dimensionsSplitOrder);
    result = EqualityUtils.hashCodeOf(result, _skipStarNodeCreationForDimensions);
    result = EqualityUtils.hashCodeOf(result, _skipMaterializationForDimensions);
    result = EqualityUtils.hashCodeOf(result, _skipMaterializationCardinalityThreshold);
    result = EqualityUtils.hashCodeOf(result, _excludeSkipMaterializationDimensionsForStarTreeIndex);
    return result;
  }
}
