/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.startree.v2.builder;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * The {@code StarTreeV2BuilderConfig} class contains the configuration for star-tree builder.
 */
public class StarTreeV2BuilderConfig {
  public static final int DEFAULT_MAX_LEAF_RECORDS = 10_000;

  // For default config, dimensions with cardinality smaller or equal to this threshold will be included into the split
  // order
  private static final int DIMENSION_CARDINALITY_THRESHOLD_FOR_DEFAULT_CONFIG = 10_000;

  private final List<String> _dimensionsSplitOrder;
  private final Set<String> _skipStarNodeCreationForDimensions;
  private final Set<AggregationFunctionColumnPair> _functionColumnPairs;
  private final int _maxLeafRecords;

  public static StarTreeV2BuilderConfig fromIndexConfig(StarTreeIndexConfig indexConfig) {
    Builder builder = new Builder();
    builder.setDimensionsSplitOrder(indexConfig.getDimensionsSplitOrder());
    List<String> skipStarNodeCreationForDimensions = indexConfig.getSkipStarNodeCreationForDimensions();
    if (skipStarNodeCreationForDimensions != null && !skipStarNodeCreationForDimensions.isEmpty()) {
      builder.setSkipStarNodeCreationForDimensions(new HashSet<>(skipStarNodeCreationForDimensions));
    }
    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    for (String functionColumnPair : indexConfig.getFunctionColumnPairs()) {
      functionColumnPairs.add(AggregationFunctionColumnPair.fromColumnName(functionColumnPair));
    }
    builder.setFunctionColumnPairs(functionColumnPairs);
    int maxLeafRecords = indexConfig.getMaxLeafRecords();
    if (maxLeafRecords > 0) {
      builder.setMaxLeafRecords(maxLeafRecords);
    }
    return builder.build();
  }

  /**
   * Generates default config based on the segment metadata.
   * <ul>
   *   <li>
   *     All dictionary-encoded single-value dimensions with cardinality smaller or equal to the threshold will be
   *     included in the split order, sorted by their cardinality in descending order.
   *   </li>
   *   <li>
   *     All dictionary-encoded Time/DateTime columns will be appended to the split order following the dimensions,
   *     sorted by their cardinality in descending order. Here we assume that time columns will be included in most
   *     queries as the range filter column and/or the group by column, so for better performance, we always include
   *     them as the last elements in the split order.
   *   </li>
   *   <li>Use COUNT(*) and SUM for all numeric metrics as function column pairs</li>
   *   <li>Use default value for max leaf records</li>
   * </ul>
   */
  public static StarTreeV2BuilderConfig generateDefaultConfig(SegmentMetadataImpl segmentMetadata) {
    Schema schema = segmentMetadata.getSchema();
    List<ColumnMetadata> dimensionColumnMetadataList = new ArrayList<>();
    List<ColumnMetadata> timeColumnMetadataList = new ArrayList<>();
    List<String> numericMetrics = new ArrayList<>();

    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField() || fieldSpec.isVirtualColumn()) {
        continue;
      }
      String column = fieldSpec.getName();
      switch (fieldSpec.getFieldType()) {
        case DIMENSION:
          ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
          if (columnMetadata.hasDictionary()
              && columnMetadata.getCardinality() <= DIMENSION_CARDINALITY_THRESHOLD_FOR_DEFAULT_CONFIG) {
            dimensionColumnMetadataList.add(columnMetadata);
          }
          break;
        case DATE_TIME:
        case TIME:
          columnMetadata = segmentMetadata.getColumnMetadataFor(column);
          if (columnMetadata.hasDictionary()) {
            timeColumnMetadataList.add(columnMetadata);
          }
          break;
        case METRIC:
          if (fieldSpec.getDataType().isNumeric()) {
            numericMetrics.add(column);
          }
      }
    }

    // Sort all dimensions/time columns with their cardinality in descending order
    dimensionColumnMetadataList.sort((o1, o2) -> Integer.compare(o2.getCardinality(), o1.getCardinality()));
    timeColumnMetadataList.sort((o1, o2) -> Integer.compare(o2.getCardinality(), o1.getCardinality()));

    List<String> dimensionsSplitOrder = new ArrayList<>();
    for (ColumnMetadata dimensionColumnMetadata : dimensionColumnMetadataList) {
      dimensionsSplitOrder.add(dimensionColumnMetadata.getColumnName());
    }
    for (ColumnMetadata timeColumnMetadata : timeColumnMetadataList) {
      dimensionsSplitOrder.add(timeColumnMetadata.getColumnName());
    }
    Preconditions.checkState(!dimensionsSplitOrder.isEmpty(), "No qualified dimension found for star-tree split order");

    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    functionColumnPairs.add(AggregationFunctionColumnPair.COUNT_STAR);
    for (String numericMetric : numericMetrics) {
      functionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, numericMetric));
    }

    return new StarTreeV2BuilderConfig(dimensionsSplitOrder, Collections.emptySet(), functionColumnPairs,
        DEFAULT_MAX_LEAF_RECORDS);
  }

  private StarTreeV2BuilderConfig(List<String> dimensionsSplitOrder, Set<String> skipStarNodeCreationForDimensions,
      Set<AggregationFunctionColumnPair> functionColumnPairs, int maxLeafRecords) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
    _functionColumnPairs = functionColumnPairs;
    _maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairs() {
    return _functionColumnPairs;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("splitOrder", _dimensionsSplitOrder)
        .append("skipStarNodeCreation", _skipStarNodeCreationForDimensions)
        .append("functionColumnPairs", _functionColumnPairs).append("maxLeafRecords", _maxLeafRecords).toString();
  }

  public static class Builder {
    private List<String> _dimensionsSplitOrder;
    private Set<String> _skipStarNodeCreationForDimensions;
    private Set<AggregationFunctionColumnPair> _functionColumnPairs;
    private int _maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

    public Builder setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
      _dimensionsSplitOrder = dimensionsSplitOrder;
      return this;
    }

    public Builder setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
      _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
      return this;
    }

    public Builder setFunctionColumnPairs(Set<AggregationFunctionColumnPair> functionColumnPairs) {
      _functionColumnPairs = functionColumnPairs;
      return this;
    }

    public Builder setMaxLeafRecords(int maxLeafRecords) {
      _maxLeafRecords = maxLeafRecords;
      return this;
    }

    public StarTreeV2BuilderConfig build() {
      if (_dimensionsSplitOrder == null || _dimensionsSplitOrder.isEmpty()) {
        throw new IllegalStateException("Illegal dimensions split order: " + _dimensionsSplitOrder);
      }
      if (_skipStarNodeCreationForDimensions == null) {
        _skipStarNodeCreationForDimensions = Collections.emptySet();
      }
      if (!_dimensionsSplitOrder.containsAll(_skipStarNodeCreationForDimensions)) {
        throw new IllegalStateException(
            "Can not skip star-node creation for dimension not in the split order, dimensionsSplitOrder: "
                + _dimensionsSplitOrder + ", skipStarNodeCreationForDimensions: " + _skipStarNodeCreationForDimensions);
      }
      if (_functionColumnPairs == null || _functionColumnPairs.isEmpty()) {
        throw new IllegalStateException("Illegal function-column pairs: " + _functionColumnPairs);
      }
      if (_maxLeafRecords <= 0) {
        throw new IllegalStateException("Illegal maximum number of leaf records: " + _maxLeafRecords);
      }
      return new StarTreeV2BuilderConfig(_dimensionsSplitOrder, _skipStarNodeCreationForDimensions,
          _functionColumnPairs, _maxLeafRecords);
    }
  }
}
