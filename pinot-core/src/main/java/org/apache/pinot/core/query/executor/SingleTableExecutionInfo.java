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
package org.apache.pinot.core.query.executor;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.BuiltInVirtualColumnDefinitions;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SingleTableExecutionInfo implements TableExecutionInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleTableExecutionInfo.class);

  private final TableDataManager _tableDataManager;
  private final List<SegmentDataManager> _segmentDataManagers;
  private final List<IndexSegment> _indexSegments;
  private final Map<IndexSegment, SegmentContext> _providedSegmentContexts;
  private final List<String> _segmentsToQuery;
  private final List<String> _optionalSegments;
  private final List<String> _notAcquiredSegments;
  @Nullable
  private final Schema _querySchema;

  public static SingleTableExecutionInfo create(InstanceDataManager instanceDataManager, String tableNameWithType,
      List<String> segmentsToQuery, List<String> optionalSegments, QueryContext queryContext)
      throws TableNotFoundException {
    return create(instanceDataManager, tableNameWithType, segmentsToQuery, optionalSegments, queryContext, null);
  }

  /// Pins native content before validating its identity. Installation changes after acquisition cannot change the
  /// pinned handle or schema. Serving-permit and deadline checks remain the responsibility of query admission.
  public static SingleTableExecutionInfo create(InstanceDataManager instanceDataManager, String tableNameWithType,
      List<String> segmentsToQuery, List<String> optionalSegments, QueryContext queryContext,
      @Nullable NativeQueryContext nativeQueryContext) throws TableNotFoundException {
    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      throw new TableNotFoundException(tableNameWithType);
    }
    if (tableDataManager.requiresQueryIdentity()) {
      if (nativeQueryContext == null) {
        throw QueryErrorCode.QUERY_VALIDATION.asException("Native queries require explicit content identities");
      }
      return createNative(tableDataManager, tableNameWithType, segmentsToQuery, optionalSegments, nativeQueryContext);
    }
    if (nativeQueryContext != null) {
      throw QueryErrorCode.QUERY_VALIDATION.asException("Table does not support native query identities");
    }

    List<String> notAcquiredSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers;
    List<IndexSegment> indexSegments;
    Map<IndexSegment, SegmentContext> providedSegmentContexts = null;

    if (!tableDataManager.isUpsertEnabled()) {
      segmentDataManagers = tableDataManager.acquireSegments(segmentsToQuery, optionalSegments, notAcquiredSegments);
      indexSegments = collectIndexSegments(segmentDataManagers);
    } else {
      TableUpsertMetadataManager tumm = tableDataManager.getTableUpsertMetadataManager();
      Preconditions.checkState(tumm != null,
          "TableUpsertMetadataManager is null for upsert-enabled table: %s", tableNameWithType);
      boolean isUsingConsistencyMode =
          tumm.getContext().getConsistencyMode() != UpsertConfig.ConsistencyMode.NONE;
      if (isUsingConsistencyMode) {
        tumm.lockForSegmentContexts();
      }
      try {
        Set<String> allSegmentsToQuery = new HashSet<>(segmentsToQuery);
        if (optionalSegments == null) {
          optionalSegments = new ArrayList<>();
        } else {
          allSegmentsToQuery.addAll(optionalSegments);
        }
        for (String segmentName : tumm.getNewlyAddedSegments()) {
          if (!allSegmentsToQuery.contains(segmentName)) {
            optionalSegments.add(segmentName);
          }
        }
        segmentDataManagers = tableDataManager.acquireSegments(segmentsToQuery, optionalSegments, notAcquiredSegments);
        indexSegments = collectIndexSegments(segmentDataManagers);
        if (isUsingConsistencyMode) {
          List<SegmentContext> segmentContexts =
              tableDataManager.getSegmentContexts(indexSegments, queryContext.getQueryOptions());
          providedSegmentContexts = new HashMap<>(segmentContexts.size());
          for (SegmentContext sc : segmentContexts) {
            providedSegmentContexts.put(sc.getIndexSegment(), sc);
          }
        }
      } finally {
        if (isUsingConsistencyMode) {
          tumm.unlockForSegmentContexts();
        }
      }
    }

    return new SingleTableExecutionInfo(tableDataManager, segmentDataManagers, indexSegments, providedSegmentContexts,
        segmentsToQuery, optionalSegments, notAcquiredSegments, null);
  }

  private static SingleTableExecutionInfo createNative(TableDataManager tableDataManager, String tableName,
      List<String> requiredSegments, @Nullable List<String> optionalSegments, NativeQueryContext context) {
    try {
      context.validateSegments(tableName, requiredSegments, optionalSegments);
    } catch (IllegalArgumentException exception) {
      throw QueryErrorCode.QUERY_VALIDATION.asException("Segment names differ from content identities", exception);
    }
    if (tableDataManager.isUpsertEnabled() || tableDataManager instanceof RealtimeTableDataManager) {
      throw QueryErrorCode.QUERY_VALIDATION.asException("Native realtime query identities are not supported yet");
    }
    List<String> missingSegments = new ArrayList<>();
    List<SegmentDataManager> managers =
        tableDataManager.acquireSegments(requiredSegments, optionalSegments, missingSegments);
    boolean accepted = false;
    try {
      Set<String> acquiredNames = new HashSet<>();
      for (SegmentDataManager manager : managers) {
        NativeQueryContext.Segment expected = context.segment(manager.getSegmentName());
        if (!(manager instanceof NativeImmutableSegmentDataManager) || expected == null
            || !acquiredNames.add(manager.getSegmentName())
            || !context.matchesInstallation(expected,
                ((NativeImmutableSegmentDataManager) manager).getInstallationIdentity())) {
          throw QueryErrorCode.SERVER_QUERY_IDENTITY_MISMATCH.asException(
              "Pinned content identity differs for segment: " + manager.getSegmentName());
        }
      }
      if (!acquiredNames.containsAll(requiredSegments)) {
        throw QueryErrorCode.SERVER_SEGMENT_MISSING.asException("Required native content is not installed");
      }
      if (managers.isEmpty()) {
        // A schema-only native request needs an authoritative catalog snapshot. An independently refreshed cache
        // cannot establish the requested table/schema incarnation, even when the scan would return zero rows.
        throw QueryErrorCode.SERVER_SEGMENT_MISSING.asException("No requested native content is installed");
      }
      Schema schema = ((NativeImmutableSegmentDataManager) managers.get(0)).getQuerySchema();
      BuiltInVirtualColumnDefinitions.addToSchema(schema);
      SingleTableExecutionInfo result = new SingleTableExecutionInfo(tableDataManager, managers,
          collectIndexSegments(managers), null, requiredSegments, optionalSegments, missingSegments, schema);
      accepted = true;
      return result;
    } finally {
      if (!accepted) {
        for (SegmentDataManager manager : managers) {
          tableDataManager.releaseSegment(manager);
        }
      }
    }
  }

  // A SegmentDataManager may expose more than one IndexSegment (e.g. DuoSegmentDataManager during a commit window,
  // where both the committed immutable and the still-mutable consuming segment must be queried together).
  private static List<IndexSegment> collectIndexSegments(List<SegmentDataManager> segmentDataManagers) {
    List<IndexSegment> indexSegments = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      if (segmentDataManager.hasMultiSegments()) {
        indexSegments.addAll(segmentDataManager.getSegments());
      } else {
        indexSegments.add(segmentDataManager.getSegment());
      }
    }
    return indexSegments;
  }

  private SingleTableExecutionInfo(TableDataManager tableDataManager, List<SegmentDataManager> segmentDataManagers,
      List<IndexSegment> indexSegments, Map<IndexSegment, SegmentContext> providedSegmentContexts,
      List<String> segmentsToQuery, List<String> optionalSegments, List<String> notAcquiredSegments,
      @Nullable Schema querySchema) {
    _tableDataManager = tableDataManager;
    _segmentDataManagers = segmentDataManagers;
    _indexSegments = indexSegments;
    _providedSegmentContexts = providedSegmentContexts;
    _segmentsToQuery = segmentsToQuery;
    _optionalSegments = optionalSegments;
    _notAcquiredSegments = notAcquiredSegments;
    _querySchema = querySchema;
  }

  public TableDataManager getTableDataManager() {
    return _tableDataManager;
  }

  @Override
  public Schema getSchema() {
    return _querySchema != null ? _querySchema : _tableDataManager.getCachedTableConfigAndSchema().getRight();
  }

  @Override
  public boolean hasRealtime() {
    return _tableDataManager instanceof RealtimeTableDataManager;
  }

  @Override
  public void releaseSegmentDataManagers() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      _tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    return _tableDataManager.getSegmentContexts(selectedSegments, queryOptions);
  }

  @Override
  public List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Nullable
  @Override
  public Map<IndexSegment, SegmentContext> getProvidedSegmentContexts() {
    return _providedSegmentContexts;
  }

  @Override
  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  @Override
  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }

  @Override
  public List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @Override
  public List<String> getNotAcquiredSegments() {
    return _notAcquiredSegments;
  }

  @Override
  public List<String> getMissingSegments() {
    return _notAcquiredSegments.stream().filter(segmentName -> !_tableDataManager.isSegmentDeletedRecently(segmentName))
        .collect(Collectors.toList());
  }

  @Override
  public int getNumSegmentsAcquired() {
    return _segmentDataManagers.size();
  }

  @Override
  public ConsumingSegmentsInfo getConsumingSegmentsInfo() {
    int numConsumingSegmentsQueried = 0;
    long minIndexTimeMs = 0;
    long minIngestionTimeMs = 0;
    long maxEndTimeMs = 0;
    if (_tableDataManager instanceof RealtimeTableDataManager) {
      minIndexTimeMs = Long.MAX_VALUE;
      minIngestionTimeMs = Long.MAX_VALUE;
      maxEndTimeMs = Long.MIN_VALUE;
      for (IndexSegment indexSegment : _indexSegments) {
        SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
        if (indexSegment instanceof MutableSegment) {
          numConsumingSegmentsQueried += 1;
          long indexTimeMs = segmentMetadata.getLastIndexedTimestamp();
          if (indexTimeMs > 0) {
            minIndexTimeMs = Math.min(minIndexTimeMs, indexTimeMs);
          }
          long ingestionTimeMs =
              ((RealtimeTableDataManager) _tableDataManager).getPartitionIngestionTimeMs(indexSegment.getSegmentName());
          if (ingestionTimeMs > 0) {
            minIngestionTimeMs = Math.min(minIngestionTimeMs, ingestionTimeMs);
          }
        } else if (indexSegment instanceof ImmutableSegment) {
          long indexCreationTime = segmentMetadata.getIndexCreationTime();
          if (indexCreationTime > 0) {
            maxEndTimeMs = Math.max(maxEndTimeMs, indexCreationTime);
          } else {
            // NOTE: the endTime may be totally inaccurate based on the value added in the timeColumn
            long endTime = segmentMetadata.getEndTime();
            if (endTime > 0) {
              maxEndTimeMs = Math.max(maxEndTimeMs, endTime);
            }
          }
        }
      }
    }

    return new ConsumingSegmentsInfo(numConsumingSegmentsQueried, minIndexTimeMs, minIngestionTimeMs, maxEndTimeMs);
  }

  @Override
  public SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService) {
    // Make a copy of the segments to avoid modifying the original list
    List<IndexSegment> indexSegments = new ArrayList<>(getIndexSegments());
    Map<IndexSegment, SegmentContext> providedSegmentContexts = getProvidedSegmentContexts();

    // Compute total docs for the table before pruning the segments
    long numTotalDocs = 0;
    for (IndexSegment indexSegment : indexSegments) {
      numTotalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
    }

    SegmentPrunerStatistics prunerStats = new SegmentPrunerStatistics();
    List<IndexSegment> selectedSegments =
        selectSegments(indexSegments, queryContext, timerContext, executorService, segmentPrunerService, prunerStats);

    int numTotalSegments = indexSegments.size();
    int numSelectedSegments = selectedSegments.size();
    LOGGER.debug("Matched {} segments after pruning", numSelectedSegments);
    List<SegmentContext> selectedSegmentContexts;
    if (providedSegmentContexts == null) {
      selectedSegmentContexts = getSegmentContexts(selectedSegments, queryContext.getQueryOptions());
    } else {
      selectedSegmentContexts = new ArrayList<>(selectedSegments.size());
      selectedSegments.forEach(s -> selectedSegmentContexts.add(providedSegmentContexts.get(s)));
    }
    return new SelectedSegmentsInfo(indexSegments, numTotalDocs, prunerStats, numTotalSegments, numSelectedSegments,
        selectedSegmentContexts);
  }
}
