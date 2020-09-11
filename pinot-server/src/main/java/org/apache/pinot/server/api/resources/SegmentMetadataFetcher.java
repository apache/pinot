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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
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

  private static final String INDEX_NOT_AVAILABLE = "NO";
  private static final String INDEX_AVAILABLE = "YES";

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
    segmentMetadataJson.set("indexes", JsonUtils.objectToJsonNode(getIndexesForSegmentColumns(segmentDataManager)));
    return JsonUtils.objectToString(segmentMetadataJson);
  }

  /**
   * Get the JSON object with the segment column's indexing metadata.
   */
  private static Map<String, Map<String, String>> getIndexesForSegmentColumns(SegmentDataManager segmentDataManager) {
    Map<String, Map<String, String>> columnIndexMap = null;
    if (segmentDataManager instanceof ImmutableSegmentDataManager) {
      ImmutableSegmentDataManager immutableSegmentDataManager = (ImmutableSegmentDataManager) segmentDataManager;
      ImmutableSegment immutableSegment = immutableSegmentDataManager.getSegment();
      if (immutableSegment instanceof ImmutableSegmentImpl) {
        ImmutableSegmentImpl immutableSegmentImpl = (ImmutableSegmentImpl) immutableSegment;
        Map<String, ColumnIndexContainer> columnIndexContainerMap = immutableSegmentImpl.getIndexContainerMap();
        columnIndexMap = getImmutableSegmentColumnIndexes(columnIndexContainerMap);
      }
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

      columnIndexMap.put(entry.getKey(), indexStatus);
    }
    return columnIndexMap;
  }
}
