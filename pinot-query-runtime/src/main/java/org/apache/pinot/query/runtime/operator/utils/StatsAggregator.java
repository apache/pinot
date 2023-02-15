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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongConsumer;
import org.apache.pinot.common.datatable.DataTable;

//TODO: Remove this and use BaseReduceService.ExecutionStatsAggregator
public class StatsAggregator {
  private long _numDocsScanned = 0L;
  private long _numEntriesScannedInFilter = 0L;
  private long _numEntriesScannedPostFilter = 0L;
  private long _numSegmentsQueried = 0L;
  private long _numSegmentsProcessed = 0L;
  private long _numSegmentsMatched = 0L;
  private long _numConsumingSegmentsQueried = 0L;
  private long _numConsumingSegmentsProcessed = 0L;
  private long _numConsumingSegmentsMatched = 0L;
  private long _minConsumingFreshnessTimeMs = Long.MAX_VALUE;
  private long _numTotalDocs = 0L;
  private long _numSegmentsPrunedByServer = 0L;
  private long _numSegmentsPrunedInvalid = 0L;
  private long _numSegmentsPrunedByLimit = 0L;
  private long _numSegmentsPrunedByValue = 0L;
  private long _explainPlanNumEmptyFilterSegments = 0L;
  private long _explainPlanNumMatchAllFilterSegments = 0L;
  private boolean _numGroupsLimitReached = false;

  public synchronized void aggregate(Map<String, String> metadata) {
    // Reduce on execution statistics.
    String numDocsScannedString = metadata.get(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName());
    if (numDocsScannedString != null) {
      _numDocsScanned += Long.parseLong(numDocsScannedString);
    }
    String numEntriesScannedInFilterString =
        metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName());
    if (numEntriesScannedInFilterString != null) {
      _numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
    }
    String numEntriesScannedPostFilterString =
        metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName());
    if (numEntriesScannedPostFilterString != null) {
      _numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
    }
    String numSegmentsQueriedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName());
    if (numSegmentsQueriedString != null) {
      _numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
    }

    String numSegmentsProcessedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName());
    if (numSegmentsProcessedString != null) {
      _numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
    }
    String numSegmentsMatchedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName());
    if (numSegmentsMatchedString != null) {
      _numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
    }

    String numConsumingSegmentsQueriedString =
        metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName());
    if (numConsumingSegmentsQueriedString != null) {
      _numConsumingSegmentsQueried += Long.parseLong(numConsumingSegmentsQueriedString);
    }

    String numConsumingSegmentsProcessed =
        metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName());
    if (numConsumingSegmentsProcessed != null) {
      _numConsumingSegmentsProcessed += Long.parseLong(numConsumingSegmentsProcessed);
    }

    String numConsumingSegmentsMatched = metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName());
    if (numConsumingSegmentsMatched != null) {
      _numConsumingSegmentsMatched += Long.parseLong(numConsumingSegmentsMatched);
    }

    String minConsumingFreshnessTimeMsString =
        metadata.get(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName());
    if (minConsumingFreshnessTimeMsString != null) {
      _minConsumingFreshnessTimeMs =
          Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), _minConsumingFreshnessTimeMs);
    }

    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER,
        l -> _numSegmentsPrunedByServer += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID,
        l -> _numSegmentsPrunedInvalid += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT,
        l -> _numSegmentsPrunedByLimit += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE,
        l -> _numSegmentsPrunedByValue += l);

    String explainPlanNumEmptyFilterSegments =
        metadata.get(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS.getName());
    if (explainPlanNumEmptyFilterSegments != null) {
      _explainPlanNumEmptyFilterSegments += Long.parseLong(explainPlanNumEmptyFilterSegments);
    }

    String explainPlanNumMatchAllFilterSegments =
        metadata.get(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS.getName());
    if (explainPlanNumMatchAllFilterSegments != null) {
      _explainPlanNumMatchAllFilterSegments += Long.parseLong(explainPlanNumMatchAllFilterSegments);
    }

    String numTotalDocsString = metadata.get(DataTable.MetadataKey.TOTAL_DOCS.getName());
    if (numTotalDocsString != null) {
      _numTotalDocs += Long.parseLong(numTotalDocsString);
    }
    _numGroupsLimitReached |=
        Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()));
  }

  public Map<String, String> getStats() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName(), String.valueOf(_numDocsScanned));
    metadata.put(DataTable.MetadataKey.TOTAL_DOCS.getName(), String.valueOf(_numTotalDocs));
    metadata.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(),
        String.valueOf(_numEntriesScannedInFilter));
    metadata.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(),
        String.valueOf(_numEntriesScannedPostFilter));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName(), String.valueOf(_numSegmentsQueried));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), String.valueOf(_numSegmentsProcessed));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName(), String.valueOf(_numSegmentsMatched));
    metadata.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(),
        String.valueOf(_numConsumingSegmentsQueried));
    metadata.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(),
        String.valueOf(_numConsumingSegmentsProcessed));
    metadata.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName(),
        String.valueOf(_numConsumingSegmentsMatched));
    metadata.put(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(),
        String.valueOf(_minConsumingFreshnessTimeMs));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER.getName(),
        String.valueOf(_numSegmentsPrunedByServer));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT.getName(),
        String.valueOf(_numSegmentsPrunedByLimit));
    metadata.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID.getName(),
        String.valueOf(_numSegmentsPrunedInvalid));
    metadata.put(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS.getName(),
        String.valueOf(_explainPlanNumEmptyFilterSegments));
    metadata.put(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS.getName(),
        String.valueOf(_explainPlanNumMatchAllFilterSegments));
    metadata.put(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), String.valueOf(_numGroupsLimitReached));

    return metadata;
  }

  private void withNotNullLongMetadata(Map<String, String> metadata, DataTable.MetadataKey key, LongConsumer consumer) {
    String strValue = metadata.get(key.getName());
    if (strValue != null) {
      consumer.accept(Long.parseLong(strValue));
    }
  }
}
