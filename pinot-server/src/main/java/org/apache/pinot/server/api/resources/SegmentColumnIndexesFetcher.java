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
  public static JsonNode getIndexesForSegmentColumns(SegmentDataManager segmentDataManager, Set<String> columnSet) {
    ArrayNode columnsIndexMetadata = JsonUtils.newArrayNode();
    if (segmentDataManager instanceof ImmutableSegmentDataManager) {
      ImmutableSegmentDataManager immutableSegmentDataManager = (ImmutableSegmentDataManager) segmentDataManager;
      ImmutableSegment immutableSegment = immutableSegmentDataManager.getSegment();
      if (immutableSegment instanceof ImmutableSegmentImpl) {
        ImmutableSegmentImpl immutableSegmentImpl = (ImmutableSegmentImpl) immutableSegment;
//        Set<String> columns = immutableSegmentImpl.getSegmentMetadata().getAllColumns();
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
      if (Objects.isNull(columnIndexContainer.getBloomFilter())) indexesNode.put("bloom-filter", "NO");
      else indexesNode.put("bloom-filter", "YES");

      if (Objects.isNull(columnIndexContainer.getDictionary())) indexesNode.put("dictionary", "NO");
      else indexesNode.put("dictionary", "YES");

      if (Objects.isNull(columnIndexContainer.getForwardIndex())) indexesNode.put("forward-index", "NO");
      else indexesNode.put("forward-index", "YES");

      if (Objects.isNull(columnIndexContainer.getInvertedIndex())) indexesNode.put("inverted-index", "NO");
      else indexesNode.put("inverted-index", "YES");

      if (Objects.isNull(columnIndexContainer.getNullValueVector()))
        indexesNode.put("null-value-vector-reader", "NO");
      else indexesNode.put("null-value-vector-reader", "YES");

      if (Objects.isNull(columnIndexContainer.getNullValueVector())) indexesNode.put("range-index", "NO");
      else indexesNode.put("range-index", "YES");

      columnIndexMap.set(e.getKey(), indexesNode);
    }
    return columnIndexMap;
  }
}
