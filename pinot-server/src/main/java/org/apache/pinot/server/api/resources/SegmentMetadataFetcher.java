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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentLoadStatus;
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
   * @param segmentDataManager
   * @param columns
   * @return
   */
  public static String getSegmentMetadata(SegmentDataManager segmentDataManager, List<String> columns) {
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
    Set<String> columnSet;
    if (columns.size() == 1 && columns.get(0).equals("*")) {
      columnSet = null;
    } else {
      columnSet = new HashSet<>(columns);
    }
    JsonNode indexes = getIndexesForSegmentColumns(segmentDataManager, columnSet);
    JsonNode segmentMetadataJson = segmentMetadata.toJson(columnSet);
    ObjectNode segmentMetadataObject = segmentMetadataJson.deepCopy();
    segmentMetadataObject.set("indexes", indexes);
    return ResourceUtils.convertToJsonString(segmentMetadataObject);
  }

  /**
   * This is a helper method to fetch segment reload status.
   * @param segmentDataManager
   * @return segment refresh time
   */
  public static SegmentLoadStatus getSegmentReloadStatus(SegmentDataManager segmentDataManager) {
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
    long refreshTime = segmentMetadata.getRefreshTime();
    return new SegmentLoadStatus(segmentDataManager.getSegmentName(), refreshTime, "");
  }

  /**
   * Get the JSON object with the segment column's indexing metadata.
   * @param segmentDataManager
   * @param columnSet
   * @return
   */
  private static JsonNode getIndexesForSegmentColumns(SegmentDataManager segmentDataManager, Set<String> columnSet) {
    ArrayNode columnsIndexMetadata = JsonUtils.newArrayNode();
    if (segmentDataManager instanceof ImmutableSegmentDataManager) {
      ImmutableSegmentDataManager immutableSegmentDataManager = (ImmutableSegmentDataManager) segmentDataManager;
      ImmutableSegment immutableSegment = immutableSegmentDataManager.getSegment();
      if (immutableSegment instanceof ImmutableSegmentImpl) {
        ImmutableSegmentImpl immutableSegmentImpl = (ImmutableSegmentImpl) immutableSegment;
        Map<String, ColumnIndexContainer> columnIndexContainerMap = immutableSegmentImpl.getIndexContainerMap();
        columnsIndexMetadata.add(getImmutableSegmentColumnIndexes(columnIndexContainerMap, columnSet));
      }
    }
    return columnsIndexMetadata;
  }

  /**
   * Helper to loop through column index container to create a index map as follows:
   * {<"bloom-filter", "YES">, <"dictionary", "NO">}
   * @param columnIndexContainerMap
   * @param columnSet
   * @return
   */
  private static ObjectNode getImmutableSegmentColumnIndexes(Map<String, ColumnIndexContainer> columnIndexContainerMap,
                                                             Set<String> columnSet) {
    ObjectNode columnIndexMap = JsonUtils.newObjectNode();
    for (Map.Entry<String, ColumnIndexContainer> e : columnIndexContainerMap.entrySet()) {
      if (columnSet != null && !columnSet.contains(e.getKey())) {
        continue;
      }
      ColumnIndexContainer columnIndexContainer = e.getValue();
      ObjectNode indexesNode = JsonUtils.newObjectNode();
      if (Objects.isNull(columnIndexContainer.getBloomFilter())) {
        indexesNode.put(BLOOM_FILTER, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(BLOOM_FILTER, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getDictionary())) {
        indexesNode.put(DICTIONARY, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(DICTIONARY, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getForwardIndex())) {
        indexesNode.put(FORWARD_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(FORWARD_INDEX, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getInvertedIndex())) {
        indexesNode.put(INVERTED_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(INVERTED_INDEX, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getNullValueVector())) {
        indexesNode.put(NULL_VALUE_VECTOR_READER, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(NULL_VALUE_VECTOR_READER, INDEX_AVAILABLE);
      }

      if (Objects.isNull(columnIndexContainer.getNullValueVector())) {
        indexesNode.put(RANGE_INDEX, INDEX_NOT_AVAILABLE);
      } else {
        indexesNode.put(RANGE_INDEX, INDEX_AVAILABLE);
      }

      columnIndexMap.set(e.getKey(), indexesNode);
    }
    return columnIndexMap;
  }
}
