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
package org.apache.pinot.core.startree.v2.store;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.segment.StarTreeMetadata;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.data.aggregator.ValueAggregatorFactory;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.data.source.ColumnDataSource;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.startree.OffHeapStarTree;
import org.apache.pinot.core.startree.StarTree;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.core.startree.v2.StarTreeV2;
import org.apache.pinot.core.startree.v2.StarTreeV2Metadata;

import static org.apache.pinot.core.startree.v2.store.StarTreeIndexMapUtils.IndexKey;
import static org.apache.pinot.core.startree.v2.store.StarTreeIndexMapUtils.IndexType;
import static org.apache.pinot.core.startree.v2.store.StarTreeIndexMapUtils.IndexValue;
import static org.apache.pinot.core.startree.v2.store.StarTreeIndexMapUtils.STAR_TREE_INDEX_KEY;


/**
 * The {@code StarTreeLoaderUtils} class provides utility methods to load star-tree indexes.
 */
public class StarTreeLoaderUtils {
  private StarTreeLoaderUtils() {
  }

  public static List<StarTreeV2> loadStarTreeV2(PinotDataBuffer dataBuffer,
      List<Map<IndexKey, IndexValue>> indexMapList, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> indexContainerMap) {
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    int numStarTrees = starTreeMetadataList.size();
    List<StarTreeV2> starTrees = new ArrayList<>(numStarTrees);

    for (int i = 0; i < numStarTrees; i++) {
      Map<IndexKey, IndexValue> indexMap = indexMapList.get(i);

      // Load star-tree index
      IndexValue indexValue = indexMap.get(STAR_TREE_INDEX_KEY);
      long start = indexValue._offset;
      long end = start + indexValue._size;
      StarTree starTree = new OffHeapStarTree(dataBuffer.view(start, end, ByteOrder.LITTLE_ENDIAN));

      StarTreeV2Metadata starTreeMetadata = starTreeMetadataList.get(i);
      int numDocs = starTreeMetadata.getNumDocs();
      Map<String, DataSource> dataSourceMap = new HashMap<>();

      // Load dimension forward indexes
      for (String dimension : starTreeMetadata.getDimensionsSplitOrder()) {
        indexValue = indexMap.get(new IndexKey(IndexType.FORWARD_INDEX, dimension));
        start = indexValue._offset;
        end = start + indexValue._size;
        ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(dimension);
        dataSourceMap.put(dimension,
            new StarTreeDimensionDataSource(dataBuffer.view(start, end, ByteOrder.BIG_ENDIAN), dimension, numDocs,
                columnMetadata.getDataType(), indexContainerMap.get(dimension).getDictionary(),
                columnMetadata.getBitsPerElement(), columnMetadata.getCardinality()));
      }

      // Load metric (function-column pair) forward indexes
      for (AggregationFunctionColumnPair functionColumnPair : starTreeMetadata.getFunctionColumnPairs()) {
        String metric = functionColumnPair.toColumnName();
        indexValue = indexMap.get(new IndexKey(IndexType.FORWARD_INDEX, metric));
        start = indexValue._offset;
        end = start + indexValue._size;
        dataSourceMap.put(metric,
            new StarTreeMetricDataSource(dataBuffer.view(start, end, ByteOrder.BIG_ENDIAN), metric, numDocs,
                ValueAggregatorFactory.getAggregatedValueType(functionColumnPair.getFunctionType())));
      }

      starTrees.add(new StarTreeV2() {
        @Override
        public StarTree getStarTree() {
          return starTree;
        }

        @Override
        public StarTreeV2Metadata getMetadata() {
          return starTreeMetadata;
        }

        @Override
        public DataSource getDataSource(String columnName) {
          return dataSourceMap.get(columnName);
        }
      });
    }

    return starTrees;
  }

  public static List<StarTreeV2> convertFromStarTreeV1(PinotDataBuffer dataBuffer, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> indexContainerMap) {
    // Load star-tree index
    StarTree starTree = new OffHeapStarTree(dataBuffer);

    // Generate star-tree V2 metadata from star-tree V1 metadata and schema
    StarTreeMetadata starTreeMetadata = segmentMetadata.getStarTreeMetadata();
    Schema schema = segmentMetadata.getSchema();
    // Add all dimensions that are not skipped for materialization into dimensions split order
    ArrayList<String> dimensionsSplitOrder = new ArrayList<>(schema.getDimensionNames());
    dimensionsSplitOrder.removeAll(starTreeMetadata.getSkipMaterializationForDimensions());
    // Add all metrics to function-column pairs
    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
      String column = metricFieldSpec.getName();
      if (metricFieldSpec.isDerivedMetric()) {
        assert metricFieldSpec.getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL;
        functionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.FASTHLL, column));
      } else {
        functionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, column));
      }
    }
    // Create the star-tree V2 metadata
    StarTreeV2Metadata starTreeV2Metadata =
        new StarTreeV2Metadata(segmentMetadata.getTotalDocs(), dimensionsSplitOrder, functionColumnPairs,
            starTreeMetadata.getMaxLeafRecords(),
            new HashSet<>(starTreeMetadata.getSkipStarNodeCreationForDimensions()));

    return Collections.singletonList(new StarTreeV2() {
      @Override
      public StarTree getStarTree() {
        return starTree;
      }

      @Override
      public StarTreeV2Metadata getMetadata() {
        return starTreeV2Metadata;
      }

      @Override
      public DataSource getDataSource(String columnName) {
        String column;
        if (columnName.contains(AggregationFunctionColumnPair.DELIMITER)) {
          column = AggregationFunctionColumnPair.fromColumnName(columnName).getColumn();
        } else {
          column = columnName;
        }
        return new ColumnDataSource(indexContainerMap.get(column), segmentMetadata.getColumnMetadataFor(column));
      }
    });
  }
}
