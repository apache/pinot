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
package org.apache.pinot.segment.local.startree.v2.builder;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
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
  private final HashMap<String, ChunkCompressionType> _functionColumnPairsConfig;
  private final int _maxLeafRecords;

  public static StarTreeV2BuilderConfig fromIndexConfig(StarTreeIndexConfig indexConfig) {
    List<String> dimensionsSplitOrder = indexConfig.getDimensionsSplitOrder();

    Set<String> skipStarNodeCreationForDimensions;
    if (indexConfig.getSkipStarNodeCreationForDimensions() != null) {
      skipStarNodeCreationForDimensions = new TreeSet<>(indexConfig.getSkipStarNodeCreationForDimensions());
      Preconditions.checkArgument(dimensionsSplitOrder.containsAll(skipStarNodeCreationForDimensions),
              "Can not skip star-node creation for dimensions not in the split order, dimensionsSplitOrder: %s, "
                      + "skipStarNodeCreationForDimensions: %s",
              dimensionsSplitOrder, skipStarNodeCreationForDimensions);
    } else {
      skipStarNodeCreationForDimensions = Collections.emptySet();
    }

    Set<AggregationFunctionColumnPair> functionColumnPairs = new TreeSet<>();
    HashMap<String, ChunkCompressionType> functionColumnPairsConfig = new HashMap<>();
    for (String functionColumnPair : indexConfig.getFunctionColumnPairs()) {
      AggregationFunctionColumnPair aggregationFunctionColumnPair = AggregationFunctionColumnPair
              .fromColumnName(functionColumnPair);
      functionColumnPairs.add(aggregationFunctionColumnPair);
      Properties propertiesFunctionColumnPairsConfig = indexConfig.getFunctionColumnPairsConfig();
      if (
              propertiesFunctionColumnPairsConfig != null
                      && propertiesFunctionColumnPairsConfig.containsKey(functionColumnPair)
      ) {
        String chunkCompressionTypeValue = propertiesFunctionColumnPairsConfig.get(functionColumnPair).toString();
        functionColumnPairsConfig.put(
                aggregationFunctionColumnPair.toColumnName(),
                ChunkCompressionType.valueOf(chunkCompressionTypeValue)
        );
      }
    }


    int maxLeafRecords = indexConfig.getMaxLeafRecords();
    if (maxLeafRecords <= 0) {
      maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;
    }

    return new StarTreeV2BuilderConfig(dimensionsSplitOrder, skipStarNodeCreationForDimensions, functionColumnPairs,
            maxLeafRecords, functionColumnPairsConfig);
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
  public static StarTreeV2BuilderConfig generateDefaultConfig(SegmentMetadata segmentMetadata) {
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
          break;
        default:
          break;
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

    Set<AggregationFunctionColumnPair> functionColumnPairs = new TreeSet<>();
    functionColumnPairs.add(AggregationFunctionColumnPair.COUNT_STAR);
    for (String numericMetric : numericMetrics) {
      functionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, numericMetric));
    }

    return new StarTreeV2BuilderConfig(dimensionsSplitOrder, Collections.emptySet(), functionColumnPairs,
            DEFAULT_MAX_LEAF_RECORDS, null);
  }

  private StarTreeV2BuilderConfig(
          List<String> dimensionsSplitOrder,
          Set<String> skipStarNodeCreationForDimensions,
          Set<AggregationFunctionColumnPair> functionColumnPairs,
          int maxLeafRecords,
          HashMap<String, ChunkCompressionType> functionColumnPairsConfig) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
    _functionColumnPairs = functionColumnPairs;
    _maxLeafRecords = maxLeafRecords;
    _functionColumnPairsConfig = functionColumnPairsConfig;
  }

  public HashMap<String, ChunkCompressionType> getFunctionColumnPairsConfig() {
    return _functionColumnPairsConfig;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StarTreeV2BuilderConfig)) {
      return false;
    }
    StarTreeV2BuilderConfig that = (StarTreeV2BuilderConfig) o;
    return _maxLeafRecords == that._maxLeafRecords
            && Objects.equals(_dimensionsSplitOrder, that._dimensionsSplitOrder)
            && Objects.equals(_skipStarNodeCreationForDimensions, that._skipStarNodeCreationForDimensions)
            && Objects.equals(_functionColumnPairs, that._functionColumnPairs)
            && Objects.equals(_functionColumnPairsConfig, that._functionColumnPairsConfig);
  }

  @Override
  public int hashCode() {
    return Objects
            .hash(
                    _dimensionsSplitOrder,
                    _skipStarNodeCreationForDimensions,
                    _functionColumnPairs,
                    _functionColumnPairsConfig,
                    _maxLeafRecords
            );
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("splitOrder", _dimensionsSplitOrder)
            .append("skipStarNodeCreation", _skipStarNodeCreationForDimensions)
            .append("functionColumnPairs", _functionColumnPairs)
            .append("maxLeafRecords", _maxLeafRecords)
            .append("functionColumnPairsConfig", _functionColumnPairsConfig).toString();
  }
}
