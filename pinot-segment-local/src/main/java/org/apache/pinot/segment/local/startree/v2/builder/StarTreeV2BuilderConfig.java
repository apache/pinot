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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
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
  private final TreeMap<AggregationFunctionColumnPair, AggregationSpec> _aggregationSpecs;
  private final int _maxLeafRecords;

  public static StarTreeV2BuilderConfig fromIndexConfig(StarTreeIndexConfig indexConfig) {
    List<String> dimensionsSplitOrder = indexConfig.getDimensionsSplitOrder();

    Set<String> skipStarNodeCreationForDimensions;
    if (indexConfig.getSkipStarNodeCreationForDimensions() != null) {
      skipStarNodeCreationForDimensions = new TreeSet<>(indexConfig.getSkipStarNodeCreationForDimensions());
      Preconditions.checkArgument(dimensionsSplitOrder.containsAll(skipStarNodeCreationForDimensions),
          "Can not skip star-node creation for dimensions not in the split order, dimensionsSplitOrder: %s, "
              + "skipStarNodeCreationForDimensions: %s", dimensionsSplitOrder, skipStarNodeCreationForDimensions);
    } else {
      skipStarNodeCreationForDimensions = Collections.emptySet();
    }
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs = new TreeMap<>();
    if (indexConfig.getFunctionColumnPairs() != null) {
      for (String functionColumnPair : indexConfig.getFunctionColumnPairs()) {
        AggregationFunctionColumnPair aggregationFunctionColumnPair =
            AggregationFunctionColumnPair.fromColumnName(functionColumnPair);
        aggregationSpecs.put(aggregationFunctionColumnPair, AggregationSpec.DEFAULT);
      }
    }
    if (indexConfig.getAggregationConfigs() != null) {
      for (StarTreeAggregationConfig aggregationConfig : indexConfig.getAggregationConfigs()) {
        AggregationFunctionColumnPair aggregationFunctionColumnPair =
            AggregationFunctionColumnPair.fromAggregationConfig(aggregationConfig);

        AggregationFunctionColumnPair valueColumnPair;
        if (aggregationConfig.getValueAggregationFunction() == null) {
          valueColumnPair = AggregationFunctionColumnPair.resolveToValueType(aggregationFunctionColumnPair);
        } else {
          valueColumnPair = new AggregationFunctionColumnPair(
              AggregationFunctionType.getAggregationFunctionType(aggregationConfig.getAggregationFunction()),
              aggregationFunctionColumnPair.getColumn());
        }
        // If there is already an equivalent functionColumnPair in the map, do not load another.
        // This prevents the duplication of the aggregation when the StarTree is constructed.
        if (aggregationSpecs.containsKey(valueColumnPair)) {
          continue;
        }
        ChunkCompressionType compressionType =
            ChunkCompressionType.valueOf(aggregationConfig.getCompressionCodec().name());
        String valueAggregationFunction = aggregationConfig.getValueAggregationFunction();
        aggregationSpecs.put(aggregationFunctionColumnPair,
            new AggregationSpec(compressionType, valueAggregationFunction));
      }
    }

    int maxLeafRecords = indexConfig.getMaxLeafRecords();
    if (maxLeafRecords <= 0) {
      maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;
    }

    return new StarTreeV2BuilderConfig(dimensionsSplitOrder, skipStarNodeCreationForDimensions, aggregationSpecs,
        maxLeafRecords);
  }

  public static StarTreeV2BuilderConfig fromMetadata(StarTreeV2Metadata starTreeV2Metadata) {
    return new StarTreeV2BuilderConfig(starTreeV2Metadata.getDimensionsSplitOrder(),
        starTreeV2Metadata.getSkipStarNodeCreationForDimensions(), starTreeV2Metadata.getAggregationSpecs(),
        starTreeV2Metadata.getMaxLeafRecords());
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

    TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs = new TreeMap<>();
    aggregationSpecs.put(AggregationFunctionColumnPair.COUNT_STAR, AggregationSpec.DEFAULT);
    for (String numericMetric : numericMetrics) {
      aggregationSpecs.put(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, numericMetric),
          AggregationSpec.DEFAULT);
    }

    return new StarTreeV2BuilderConfig(dimensionsSplitOrder, Collections.emptySet(), aggregationSpecs,
        DEFAULT_MAX_LEAF_RECORDS);
  }

  private StarTreeV2BuilderConfig(List<String> dimensionsSplitOrder, Set<String> skipStarNodeCreationForDimensions,
      TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs, int maxLeafRecords) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
    _skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
    _aggregationSpecs = aggregationSpecs;
    _maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public TreeMap<AggregationFunctionColumnPair, AggregationSpec> getAggregationSpecs() {
    return _aggregationSpecs;
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairs() {
    return _aggregationSpecs.keySet();
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  /**
   * Writes the metadata which is used to initialize the {@link StarTreeV2Metadata} when loading the segment.
   */
  public void writeMetadata(Configuration metadataProperties, int totalDocs) {
    metadataProperties.setProperty(MetadataKey.TOTAL_DOCS, totalDocs);
    metadataProperties.setProperty(MetadataKey.DIMENSIONS_SPLIT_ORDER, _dimensionsSplitOrder);
    metadataProperties.setProperty(MetadataKey.FUNCTION_COLUMN_PAIRS, _aggregationSpecs.keySet());
    metadataProperties.setProperty(MetadataKey.AGGREGATION_COUNT, _aggregationSpecs.size());
    int index = 0;
    for (Map.Entry<AggregationFunctionColumnPair, AggregationSpec> entry : _aggregationSpecs.entrySet()) {
      AggregationFunctionColumnPair functionColumnPair = entry.getKey();
      AggregationSpec aggregationSpec = entry.getValue();
      String prefix = MetadataKey.AGGREGATION_PREFIX + index + '.';
      metadataProperties.setProperty(prefix + MetadataKey.FUNCTION_TYPE,
          functionColumnPair.getFunctionType().getName());
      metadataProperties.setProperty(prefix + MetadataKey.COLUMN_NAME, functionColumnPair.getColumn());
      metadataProperties.setProperty(prefix + MetadataKey.COMPRESSION_CODEC, aggregationSpec.getCompressionType());
      String valueFunctionType = aggregationSpec.getValueAggregationFunctionTypeName();
      metadataProperties.setProperty(prefix + MetadataKey.VALUE_FUNCTION_TYPE, valueFunctionType);
      index++;
    }
    metadataProperties.setProperty(MetadataKey.MAX_LEAF_RECORDS, _maxLeafRecords);
    metadataProperties.setProperty(MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
        _skipStarNodeCreationForDimensions);
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
    return _maxLeafRecords == that._maxLeafRecords && Objects.equals(_dimensionsSplitOrder, that._dimensionsSplitOrder)
        && Objects.equals(_skipStarNodeCreationForDimensions, that._skipStarNodeCreationForDimensions)
        && Objects.equals(_aggregationSpecs, that._aggregationSpecs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_dimensionsSplitOrder, _skipStarNodeCreationForDimensions, _aggregationSpecs, _maxLeafRecords);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("splitOrder", _dimensionsSplitOrder)
        .append("skipStarNodeCreation", _skipStarNodeCreationForDimensions)
        .append("aggregationSpecs", _aggregationSpecs).append("maxLeafRecords", _maxLeafRecords).toString();
  }
}
