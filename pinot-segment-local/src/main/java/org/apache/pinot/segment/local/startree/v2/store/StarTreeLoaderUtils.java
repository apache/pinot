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
package org.apache.pinot.segment.local.startree.v2.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.local.startree.OffHeapStarTree;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;


/**
 * The {@code StarTreeLoaderUtils} class provides utility methods to load star-tree indexes.
 */
public class StarTreeLoaderUtils {
  private StarTreeLoaderUtils() {
  }

  public static List<StarTreeV2> loadStarTreeV2(SegmentDirectory.Reader segmentReader,
      SegmentMetadataImpl segmentMetadata, Map<String, ColumnIndexContainer> indexContainerMap)
      throws IOException {
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    int numStarTrees = starTreeMetadataList.size();
    List<StarTreeV2> starTrees = new ArrayList<>(numStarTrees);
    for (int i = 0; i < numStarTrees; i++) {
      SegmentDirectory.Reader indexReader = segmentReader.getStarTreeIndexReader(i);
      // Load star-tree index
      StarTree starTree = new OffHeapStarTree(indexReader.getIndexFor(String.valueOf(i), StandardIndexes.inverted()));

      StarTreeV2Metadata starTreeMetadata = starTreeMetadataList.get(i);
      int numDocs = starTreeMetadata.getNumDocs();
      Map<String, DataSource> dataSourceMap = new HashMap<>();

      // Load dimension forward indexes
      for (String dimension : starTreeMetadata.getDimensionsSplitOrder()) {
        PinotDataBuffer forwardIndexDataBuffer = indexReader.getIndexFor(dimension, StandardIndexes.forward());
        ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(dimension);
        FixedBitSVForwardIndexReaderV2 forwardIndex =
            new FixedBitSVForwardIndexReaderV2(forwardIndexDataBuffer, numDocs, columnMetadata.getBitsPerElement());
        dataSourceMap.put(dimension, new StarTreeDataSource(columnMetadata.getFieldSpec(), numDocs, forwardIndex,
            indexContainerMap.get(dimension).getIndex(StandardIndexes.dictionary())));
      }

      // Load metric (function-column pair) forward indexes
      for (Map.Entry<String, AggregationFunctionColumnPair> functionColumnPair : starTreeMetadata
          .getFunctionColumnPairs().entrySet()) {
        String metric = functionColumnPair.getValue().toColumnName();
        PinotDataBuffer forwardIndexDataBuffer = indexReader.getIndexFor(metric, StandardIndexes.forward());
        DataType dataType =
            ValueAggregatorFactory.getAggregatedValueType(functionColumnPair.getValue().getFunctionType());
        FieldSpec fieldSpec = new MetricFieldSpec(metric, dataType);
        ForwardIndexReader<?> forwardIndex =
            ForwardIndexReaderFactory.createRawIndexReader(forwardIndexDataBuffer, dataType.getStoredType(), true);
        dataSourceMap.put(metric, new StarTreeDataSource(fieldSpec, numDocs, forwardIndex, null));
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

        @Override
        public void close()
            throws IOException {
          // NOTE: Close the indexes managed by the star-tree (dictionary is managed inside the ColumnIndexContainer).
          for (DataSource dataSource : dataSourceMap.values()) {
            // Forward index cannot be null here
            dataSource.getForwardIndex().close();
          }
        }
      });
    }
    return starTrees;
  }
}
