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
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * This is a wrapper class for fetching segment metadata related information.
 */
public class SegmentMetadataFetcher {
  private SegmentMetadataFetcher() {
  }

  private static final String COLUMN_INDEX_KEY = "indexes";
  private static final String STAR_TREE_INDEX_KEY = "star-tree-index";

  private static final String BLOOM_FILTER = "bloom-filter";
  private static final String DICTIONARY = "dictionary";
  private static final String FORWARD_INDEX = "forward-index";
  private static final String INVERTED_INDEX = "inverted-index";
  private static final String NULL_VALUE_VECTOR_READER = "null-value-vector-reader";
  private static final String RANGE_INDEX = "range-index";
  private static final String JSON_INDEX = "json-index";
  private static final String H3_INDEX = "h3-index";
  private static final String FST_INDEX = "fst-index";
  private static final String TEXT_INDEX = "text-index";

  private static final String INDEX_NOT_AVAILABLE = "NO";
  private static final String INDEX_AVAILABLE = "YES";

  private static final String STAR_TREE_DIMENSION_COLUMNS = "dimension-columns";
  private static final String STAR_TREE_METRIC_AGGREGATIONS = "metric-aggregations";
  private static final String STAR_TREE_MAX_LEAF_RECORDS = "max-leaf-records";
  private static final String STAR_TREE_DIMENSION_COLUMNS_SKIPPED = "dimension-columns-skipped";

  /**
   * This is a helper method that fetches the segment metadata for a given segment.
   * @param columns Columns to include for metadata
   */
  public static String getSegmentMetadata(SegmentDataManager segmentDataManager, List<String> columns)
      throws JsonProcessingException {
    IndexSegment segment = segmentDataManager.getSegment();
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    Set<String> columnSet;
    if (columns.size() == 1 && columns.get(0).equals("*")) {
      // Making code consistent and returning metadata and indexes only for non-virtual columns.
      columnSet = segment.getPhysicalColumnNames();
    } else {
      columnSet = new HashSet<>(columns);
    }
    ObjectNode segmentMetadataJson = (ObjectNode) segmentMetadata.toJson(columnSet);
    segmentMetadataJson
        .set(COLUMN_INDEX_KEY, JsonUtils.objectToJsonNode(getIndexesForSegmentColumns(segmentDataManager, columnSet)));
    segmentMetadataJson
        .set(STAR_TREE_INDEX_KEY, JsonUtils.objectToJsonNode((getStarTreeIndexesForSegment(segmentDataManager))));
    return JsonUtils.objectToString(segmentMetadataJson);
  }

  /**
   * Get the JSON object with the segment column's indexing metadata.
   * Lists all the columns if the parameter columnSet is null.
   */
  private static Map<String, Map<String, String>> getIndexesForSegmentColumns(SegmentDataManager segmentDataManager,
      @Nullable Set<String> columnSet) {
    IndexSegment segment = segmentDataManager.getSegment();
    Map<String, Map<String, String>> columnIndexMap = new LinkedHashMap<>();
    for (String physicalColumnName : segment.getPhysicalColumnNames()) {
      if (columnSet == null || columnSet.contains(physicalColumnName)) {
        DataSource dataSource = segment.getDataSource(physicalColumnName);
        columnIndexMap.put(physicalColumnName, getColumnIndexes(dataSource));
      }
    }
    return columnIndexMap;
  }

  /**
   * Helper to loop through column datasource to create a index map as follows for each column:
   * {<"bloom-filter", "YES">, <"dictionary", "NO">}
   */
  private static Map<String, String> getColumnIndexes(DataSource dataSource) {
    Map<String, String> indexStatus = new LinkedHashMap<>();
    if (Objects.isNull(dataSource.getBloomFilter())) {
      indexStatus.put(BLOOM_FILTER, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(BLOOM_FILTER, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getDictionary())) {
      indexStatus.put(DICTIONARY, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(DICTIONARY, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getForwardIndex())) {
      indexStatus.put(FORWARD_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(FORWARD_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getInvertedIndex())) {
      indexStatus.put(INVERTED_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(INVERTED_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getNullValueVector())) {
      indexStatus.put(NULL_VALUE_VECTOR_READER, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(NULL_VALUE_VECTOR_READER, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getRangeIndex())) {
      indexStatus.put(RANGE_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(RANGE_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getJsonIndex())) {
      indexStatus.put(JSON_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(JSON_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getH3Index())) {
      indexStatus.put(H3_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(H3_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getFSTIndex())) {
      indexStatus.put(FST_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(FST_INDEX, INDEX_AVAILABLE);
    }

    if (Objects.isNull(dataSource.getTextIndex())) {
      indexStatus.put(TEXT_INDEX, INDEX_NOT_AVAILABLE);
    } else {
      indexStatus.put(TEXT_INDEX, INDEX_AVAILABLE);
    }

    return indexStatus;
  }

  /**
   * Get the JSON object containing star tree index details for a segment.
   */
  @Nullable
  private static List<Map<String, Object>> getStarTreeIndexesForSegment(SegmentDataManager segmentDataManager) {
    List<StarTreeV2> starTrees = segmentDataManager.getSegment().getStarTrees();
    return starTrees != null ? getStarTreeIndexes(starTrees) : null;
  }

  /**
   * Helper to loop over star trees of a segment to create a map containing star tree details.
   */
  private static List<Map<String, Object>> getStarTreeIndexes(List<StarTreeV2> starTrees) {
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
      starTreeIndexMap
          .put(STAR_TREE_DIMENSION_COLUMNS_SKIPPED, starTreeMetadata.getSkipStarNodeCreationForDimensions());
      startreeDetails.add(starTreeIndexMap);
    }
    return startreeDetails;
  }
}
