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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


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
        AggregationFunctionColumnPair functionColumnPair = new AggregationFunctionColumnPair(functionType, columnName);
        // Lookup the stored aggregation type
        AggregationFunctionColumnPair storedType =
            AggregationFunctionColumnPair.resolveToStoredType(functionColumnPair);

        Map<String, Object> functionParameters = new HashMap<>();
        for (Iterator<String> it = aggregationConfig.getKeys(MetadataKey.FUNCTION_PARAMETERS); it.hasNext(); ) {
          String key = it.next();
          functionParameters.put(StringUtils.removeStart(key, MetadataKey.FUNCTION_PARAMETERS + '.'),
              aggregationConfig.getProperty(key));
        }

        AggregationSpec aggregationSpec =
            new AggregationSpec(aggregationConfig.getEnum(MetadataKey.COMPRESSION_CODEC, CompressionCodec.class, null),
                aggregationConfig.getBoolean(MetadataKey.DERIVE_NUM_DOCS_PER_CHUNK, null),
                aggregationConfig.getInteger(MetadataKey.INDEX_VERSION, null),
                aggregationConfig.getInteger(MetadataKey.TARGET_MAX_CHUNK_SIZE_BYTES, null),
                aggregationConfig.getInteger(MetadataKey.TARGET_DOCS_PER_CHUNK, null), functionParameters);
        // If there is already an equivalent functionColumnPair in the map for the stored type, do not load another.
        _aggregationSpecs.putIfAbsent(storedType, aggregationSpec);
      }
    } else {
      // Backward compatibility with columnName format
      for (String functionColumnPairName : metadataProperties.getStringArray(MetadataKey.FUNCTION_COLUMN_PAIRS)) {
        AggregationFunctionColumnPair functionColumnPair =
            AggregationFunctionColumnPair.fromColumnName(functionColumnPairName);
        // Lookup the stored aggregation type
        AggregationFunctionColumnPair storedType =
            AggregationFunctionColumnPair.resolveToStoredType(functionColumnPair);
        // If there is already an equivalent functionColumnPair in the map for the stored type, do not load another.
        _aggregationSpecs.putIfAbsent(storedType, AggregationSpec.DEFAULT);
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
    return _aggregationSpecs.containsKey(functionColumnPair);
  }

  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return _skipStarNodeCreationForDimensions;
  }

  public static void writeMetadata(Configuration metadataProperties, int totalDocs, List<String> dimensionsSplitOrder,
      TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs, int maxLeafRecords,
      Set<String> skipStarNodeCreationForDimensions) {
    metadataProperties.setProperty(MetadataKey.TOTAL_DOCS, totalDocs);
    metadataProperties.setProperty(MetadataKey.DIMENSIONS_SPLIT_ORDER, dimensionsSplitOrder);
    metadataProperties.setProperty(MetadataKey.FUNCTION_COLUMN_PAIRS, aggregationSpecs.keySet());
    metadataProperties.setProperty(MetadataKey.AGGREGATION_COUNT, aggregationSpecs.size());
    int index = 0;
    for (Map.Entry<AggregationFunctionColumnPair, AggregationSpec> entry : aggregationSpecs.entrySet()) {
      AggregationFunctionColumnPair functionColumnPair = entry.getKey();
      AggregationSpec aggregationSpec = entry.getValue();
      String prefix = MetadataKey.AGGREGATION_PREFIX + index + '.';
      metadataProperties.setProperty(prefix + MetadataKey.FUNCTION_TYPE,
          functionColumnPair.getFunctionType().getName());
      metadataProperties.setProperty(prefix + MetadataKey.COLUMN_NAME, functionColumnPair.getColumn());

      for (Map.Entry<String, Object> parameters : aggregationSpec.getFunctionParameters().entrySet()) {
        metadataProperties.setProperty(prefix + MetadataKey.FUNCTION_PARAMETERS + '.' + parameters.getKey(),
            parameters.getValue());
      }

      metadataProperties.setProperty(prefix + MetadataKey.COMPRESSION_CODEC, aggregationSpec.getCompressionCodec());
      metadataProperties.setProperty(prefix + MetadataKey.DERIVE_NUM_DOCS_PER_CHUNK,
          aggregationSpec.isDeriveNumDocsPerChunk());
      metadataProperties.setProperty(prefix + MetadataKey.INDEX_VERSION, aggregationSpec.getIndexVersion());
      metadataProperties.setProperty(prefix + MetadataKey.TARGET_MAX_CHUNK_SIZE_BYTES,
          aggregationSpec.getTargetMaxChunkSizeBytes());
      metadataProperties.setProperty(prefix + MetadataKey.TARGET_DOCS_PER_CHUNK,
          aggregationSpec.getTargetDocsPerChunk());
      index++;
    }
    metadataProperties.setProperty(MetadataKey.MAX_LEAF_RECORDS, maxLeafRecords);
    metadataProperties.setProperty(MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
        skipStarNodeCreationForDimensions);
  }
}
