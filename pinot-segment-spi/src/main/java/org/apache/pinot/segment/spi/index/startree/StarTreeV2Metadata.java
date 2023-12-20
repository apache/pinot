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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;


/**
 * The {@code StarTreeV2Metadata} contains the metadata for a single star-tree.
 */
public class StarTreeV2Metadata {
  private final int _numDocs;
  private final List<String> _dimensionsSplitOrder;
  private final TreeMap<AggregationFunctionColumnPair, AggregationSpec> _aggregationSpecs;

  // The following properties are useful for generating the builder config
  private final int _maxLeafRecords;
  private final Set<String> _skipStarNodeCreationForDimensions;

  public StarTreeV2Metadata(Configuration metadataProperties) {
    _numDocs = metadataProperties.getInt(MetadataKey.TOTAL_DOCS);
    _dimensionsSplitOrder = Arrays.asList(metadataProperties.getStringArray(MetadataKey.DIMENSIONS_SPLIT_ORDER));
    _aggregationSpecs = new TreeMap<>();
    int numAggregations = metadataProperties.getInt(MetadataKey.AGGREGATION_COUNT, 0);
    if (numAggregations > 0) {
      for (int i = 0; i < numAggregations; i++) {
        Configuration aggregationConfig = metadataProperties.subset(MetadataKey.AGGREGATION_PREFIX + i);
        AggregationFunctionType functionType =
            AggregationFunctionType.getAggregationFunctionType(aggregationConfig.getString(MetadataKey.FUNCTION_TYPE));
        String columnName = aggregationConfig.getString(MetadataKey.COLUMN_NAME);
        ChunkCompressionType compressionType =
            ChunkCompressionType.valueOf(aggregationConfig.getString(MetadataKey.COMPRESSION_CODEC));
        _aggregationSpecs.put(new AggregationFunctionColumnPair(functionType, columnName),
            new AggregationSpec(compressionType));
      }
    } else {
      // Backward compatibility with columnName format
      for (String functionColumnPairName : metadataProperties.getStringArray(MetadataKey.FUNCTION_COLUMN_PAIRS)) {
        AggregationFunctionColumnPair functionColumnPair =
            AggregationFunctionColumnPair.fromColumnName(functionColumnPairName);
        _aggregationSpecs.put(functionColumnPair, AggregationSpec.DEFAULT);
      }
    }
    _maxLeafRecords = metadataProperties.getInt(MetadataKey.MAX_LEAF_RECORDS);
    _skipStarNodeCreationForDimensions = new HashSet<>(
        Arrays.asList(metadataProperties.getStringArray(MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS)));
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public TreeMap<AggregationFunctionColumnPair, AggregationSpec> getAggregationSpecs() {
    return _aggregationSpecs;
  }

  public Set<AggregationFunctionColumnPair> getFunctionColumnPairs() {
    return _aggregationSpecs.keySet();
  }

  public boolean containsFunctionColumnPair(AggregationFunctionColumnPair functionColumnPair) {
    boolean containsColumnPair = _aggregationSpecs.containsKey(functionColumnPair);
    if (!containsColumnPair) {
      AggregationFunctionColumnPair valuePair = AggregationFunctionColumnPair.resolveToValueType(functionColumnPair);
      containsColumnPair = _aggregationSpecs.containsKey(valuePair);
    }
    return containsColumnPair;
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }
}
