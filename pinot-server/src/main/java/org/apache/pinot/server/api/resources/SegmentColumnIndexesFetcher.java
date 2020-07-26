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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.spi.utils.JsonUtils;

public class SegmentColumnIndexesFetcher {
  private static final String BLOOM_FILTER = "bloom-filter";
  private static final String DICTIONARY = "dictionary";
  private static final String FORWARD_INDEX = "forward-index";
  private static final String INVERTED_INDEX = "inverted-index";
  private static final String NULL_VALUE_VECTOR_READER = "null-value-vector-reader";
  private static final String RANGE_INDEX = "range-index";

  private static final String INDEX_NOT_AVAILABLE = "NO";
  private static final String INDEX_AVAILABLE = "YES";

  public static JsonNode getIndexesForSegmentColumns(SegmentDataManager segmentDataManager, Set<String> columnSet) {
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
