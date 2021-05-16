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
package org.apache.pinot.server.api.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.utils.JsonUtils;

/**
 * This is a wrapper class for fetching segment metadata related information.
 */
public class SegmentMetadataFetcher {
  private static final String BLOOM_FILTER = "bloom-filter";
  private static final String DICTIONARY = "dictionary";
  private static final String FORWARD_INDEX = "forward-index";
  private static final String INVERTED_INDEX = "inverted-index";
  private static final String NULL_VALUE_VECTOR_READER = "null-value-vector-reader";
  private static final String RANGE_INDEX = "range-index";
  private static final String JSON_INDEX = "json-index";

  private static final String INDEX_NOT_AVAILABLE = "NO";
  private static final String INDEX_AVAILABLE = "YES";

  private static final String STAR_TREE_INDEX_KEY = "star-tree-index";
  private static final String STAR_TREE_DIMENSION_COLUMNS = "dimension-columns";
  private static final String STAR_TREE_METRIC_AGGREGATIONS = "metric-aggregations";
  private static final String STAR_TREE_MAX_LEAF_RECORDS = "max-leaf-records";
  private static final String STAR_TREE_DIMENSION_COLUMNS_SKIPPED = "dimension-columns-skipped";

  private static final String COLUMN_INDEX_KEY = "indexes";
  

  /**
   * This is a helper method that fetches the segment metadata for a given segment.
   * @param columns Columns to include for metadata
   */
  public static String getSegmentMetadata(SegmentDataManager segmentDataManager, List<String> columns)
      throws JsonProcessingException {
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
    Set<String> columnSet;
    if (columns.size() == 1 && columns.get(0).equals("*")) {
      columnSet = null;
    } else {
      columnSet = new HashSet<>(columns);
    }
    ObjectNode segmentMetadataJson = (ObjectNode) segmentMetadata.toJson(columnSet);
    segmentMetadataJson.set(COLUMN_INDEX_KEY, JsonUtils.objectToJsonNode(getIndexesForSegmentColumns(segmentDataManager)));
    segmentMetadataJson.set(STAR_TREE_INDEX_KEY, JsonUtils.objectToJsonNode((getStartreeIndexForSegmentColumns(segmentDataManager))));
    return JsonUtils.objectToString(segmentMetadataJson);
  }

  /**
   * Get the JSON object with the segment column's indexing metadata.
   */
  private static Map<String, Map<String, String>> getIndexesForSegmentColumns(SegmentDataManager segmentDataManager) {
    Map<String, Map<String, String>> columnIndexMap = null;
    IndexSegment segment = segmentDataManager.getSegment();
    if (segment instanceof ImmutableSegmentImpl) {
      return getImmutableSegmentColumnIndexes(((ImmutableSegmentImpl) segment).getIndexContainerMap());
    } else {
      return null;
    }
    return columnIndexMap;
  }

  /**
   * Helper to loop through column index container to create a index map as follows for each column:
   * {<"bloom-filter", "YES">, <"dictionary", "NO">}
   */
  private static Map<String, Map<String, String>> getImmutableSegmentColumnIndexes(Map<String, ColumnIndexContainer> columnIndexContainerMap) {
    Map<String, Map<String, String>> columnIndexMap = new LinkedHashMap<>();
    for (Map.Entry<String, ColumnIndexContainer> entry : columnIndexContainerMap.entrySet()) {
      ColumnIndexContainer columnIndexContainer = entry.getValue();
      Map<String, String> indexStatus = new LinkedHashMap<>();
      if (Objects.isNull(columnIndexContainer.getBloomFilter())) {
        indexStatus.put(BLOOM_FILTER, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(BLOOM_FILTER, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getDictionary())) {
        indexStatus.put(DICTIONARY, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(DICTIONARY, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getForwardIndex())) {
        indexStatus.put(FORWARD_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(FORWARD_INDEX, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getInvertedIndex())) {
        indexStatus.put(INVERTED_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(INVERTED_INDEX, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getNullValueVector())) {
        indexStatus.put(NULL_VALUE_VECTOR_READER, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(NULL_VALUE_VECTOR_READER, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getNullValueVector())) {
        indexStatus.put(RANGE_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(RANGE_INDEX, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getJsonIndex())){
        indexStatus.put(JSON_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexStatus.put(JSON_INDEX, INDEX_AVAILABLE);
      }

      columnIndexMap.put(entry.getKey(), indexStatus);
    }
    return columnIndexMap;
  }

  /**
   * Get the JSON object containing star tree index details for a segment.
   */
  private static List<Map<String, Object>> getStartreeIndexForSegmentColumns(SegmentDataManager segmentDataManager) {
    List<StarTreeV2> starTrees = segmentDataManager.getSegment().getStarTrees();
    return starTrees != null ? getImmutableSegmentStartreeIndexes(starTrees) : null;
  }

  /**
   * Helper to loop over star trees of a segment to create a map containing star tree details.
   */
  private static List<Map<String, Object>> getImmutableSegmentStartreeIndexes(List<StarTreeV2> starTrees){
    List<Map<String, Object>> startreeDetails = new ArrayList<>();
    for (StarTreeV2 starTree : starTrees) {
      StarTreeV2Metadata starTreeMetadata = starTree.getMetadata();

      Map<String, Object> starTreeIndexMap = new LinkedHashMap<>();

      List<String> starTreeDimensions = starTreeMetadata.getDimensionsSplitOrder();
      starTreeIndexMap.put(STAR_TREE_DIMENSION_COLUMNS, starTreeDimensions);

      List<String> starTreeMetricAggregations = new ArrayList<>();
      Set<AggregationFunctionColumnPair> functionColumnPairs = starTreeMetadata.getFunctionColumnPairs();
      for (AggregationFunctionColumnPair functionColumnPair : functionColumnPairs) {
        starTreeMetricAggregations.add(functionColumnPair.toColumnName());
      }
      starTreeIndexMap.put(STAR_TREE_METRIC_AGGREGATIONS, starTreeMetricAggregations);

      starTreeIndexMap.put(STAR_TREE_MAX_LEAF_RECORDS, starTreeMetadata.getMaxLeafRecords());
      starTreeIndexMap.put(STAR_TREE_DIMENSION_COLUMNS_SKIPPED, starTreeMetadata.getSkipStarNodeCreationForDimensions());
      startreeDetails.add(starTreeIndexMap);
    }
    return startreeDetails;
  }
}
