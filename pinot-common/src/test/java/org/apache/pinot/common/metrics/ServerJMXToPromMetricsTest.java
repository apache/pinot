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

package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";
  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setup() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _serverMetrics = new ServerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  /**
   * This test validates each timer defined in {@link ServerTimer}
   */
  @Test
  public void serverTimerTest()
      throws IOException, URISyntaxException {

    //get all exposed metrics before we expose any timers
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    for (ServerTimer serverTimer : ServerTimer.values()) {
      if (serverTimer.isGlobal()) {
        _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
        //this gauge uses rawTableName
      } else if (serverTimer == ServerTimer.SEGMENT_UPLOAD_TIME_MS) {
        _serverMetrics.addTimedTableValue(RAW_TABLE_NAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      } else {
        _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      }
    }
    //assert on timers with labels
    assertTimerExportedCorrectly("freshnessLagMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("nettyConnectionSendResponseLatency", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("executionThreadCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("systemActivitiesCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("responseSerCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("segmentUploadTimeMs", List.of("table", RAW_TABLE_NAME), EXPORTED_METRIC_PREFIX);

    assertTimerExportedCorrectly("totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("upsertPreloadTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("upsertRemoveExpiredPrimaryKeysTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("grpcQueryExecutionMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("upsertSnapshotTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("dedupRemoveExpiredPrimaryKeysTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("secondaryQWaitTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    //assert on all global timers
    assertTimerExportedCorrectly("hashJoinBuildTableCpuTimeMs", EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("multiStageSerializationCpuTimeMs", EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("multiStageDeserializationCpuTimeMs", EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("receiveDownstreamWaitCpuTimeMs", EXPORTED_METRIC_PREFIX);
    assertTimerExportedCorrectly("receiveUpstreamWaitCpuTimeMs", EXPORTED_METRIC_PREFIX);

    //now assert that we've added exported all timers present in ServerTimer.java
    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(),
        ServerTimer.values().length * TIMER_TYPES.size());
  }

  /**
   * This test validates each meter defined in {@link ServerMeter}
   */
  @Test
  public void serverMeterTest()
      throws Exception {

    //get all exposed metrics before we expose any meters
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    addGlobalMeter(ServerMeter.QUERIES);
    assertMeterExportedCorrectly("queries", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.UNCAUGHT_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_uncaught", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_requestDeserialization", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_responseSerialization", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.QUERY_EXECUTION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_queryExecution", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS);
    assertMeterExportedCorrectly("helix_zookeeperReconnects", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.REALTIME_ROWS_CONSUMED);
    assertMeterExportedCorrectly("realtime_rowsConsumed", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_consumptionExceptions", EXPORTED_METRIC_PREFIX);

    //todo: REALTIME_OFFSET_COMMITS and REALTIME_OFFSET_COMMIT_EXCEPTIONS are not used anywhere right now. This test
    // case might need to be changed depending on how this metric is used in future
    addGlobalMeter(ServerMeter.REALTIME_OFFSET_COMMITS);
    assertMeterExportedCorrectly("realtime_offsetCommits", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.REALTIME_OFFSET_COMMIT_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_realtimeOffsetCommit", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_SENT);
    assertMeterExportedCorrectly("llcControllerResponse_NotSent", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT);
    assertMeterExportedCorrectly("llcControllerResponse_Commit", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_HOLD);
    assertMeterExportedCorrectly("llcControllerResponse_Hold", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_CATCH_UP);
    assertMeterExportedCorrectly("llcControllerResponse_CatchUp", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_DISCARD);
    assertMeterExportedCorrectly("llcControllerResponse_Discard", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_KEEP);
    assertMeterExportedCorrectly("llcControllerResponse_Keep", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_LEADER);
    assertMeterExportedCorrectly("llcControllerResponse_NotLeader", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_FAILED);
    assertMeterExportedCorrectly("llcControllerResponse_Failed", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS);
    assertMeterExportedCorrectly("llcControllerResponse_CommitSuccess", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_CONTINUE);
    assertMeterExportedCorrectly("llcControllerResponse_CommitContinue", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_PROCESSED);
    assertMeterExportedCorrectly("llcControllerResponse_Processed", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.LLC_CONTROLLER_RESPONSE_UPLOAD_SUCCESS);
    assertMeterExportedCorrectly("llcControllerResponse_UploadSuccess", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.NO_TABLE_ACCESS);
    assertMeterExportedCorrectly("noTableAccess", EXPORTED_METRIC_PREFIX);

    addGlobalMeter(ServerMeter.INDEXING_FAILURES);
    addGlobalMeter(ServerMeter.READINESS_CHECK_OK_CALLS);
    addGlobalMeter(ServerMeter.READINESS_CHECK_BAD_CALLS);
    addGlobalMeter(ServerMeter.QUERIES_KILLED);
    addGlobalMeter(ServerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED);
    addGlobalMeter(ServerMeter.HEAP_PANIC_LEVEL_EXCEEDED);
    addGlobalMeter(ServerMeter.NETTY_CONNECTION_BYTES_RECEIVED);
    addGlobalMeter(ServerMeter.NETTY_CONNECTION_RESPONSES_SENT);
    addGlobalMeter(ServerMeter.NETTY_CONNECTION_BYTES_SENT);
    addGlobalMeter(ServerMeter.GRPC_QUERIES);
    addGlobalMeter(ServerMeter.GRPC_BYTES_RECEIVED);
    addGlobalMeter(ServerMeter.GRPC_BYTES_SENT);
    addGlobalMeter(ServerMeter.GRPC_TRANSPORT_READY);
    addGlobalMeter(ServerMeter.GRPC_TRANSPORT_TERMINATED);

    addGlobalMeter(ServerMeter.HASH_JOIN_TIMES_MAX_ROWS_REACHED);
    addGlobalMeter(ServerMeter.AGGREGATE_TIMES_NUM_GROUPS_LIMIT_REACHED);
    addGlobalMeter(ServerMeter.MULTI_STAGE_IN_MEMORY_MESSAGES);
    addGlobalMeter(ServerMeter.MULTI_STAGE_RAW_MESSAGES);
    addGlobalMeter(ServerMeter.MULTI_STAGE_RAW_BYTES);
    addGlobalMeter(ServerMeter.WINDOW_TIMES_MAX_ROWS_REACHED);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_SANITIZED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsSanitized", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FETCHED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsFetched", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FILTERED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsFiltered", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.INVALID_REALTIME_ROWS_DROPPED, CLIENT_ID);
    assertMeterExportedCorrectly("invalidRealtimeRowsDropped", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("incompleteRealtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("realtimeConsumptionExceptions", List.of("table", "myTable_REALTIME_myTopic"),
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_MERGED_TEXT_IDX_TRUNCATED_DOCUMENT_SIZE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("realtimeMergedTextIdxTruncatedDocumentSize", List.of("table", "myTable"),
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.QUERIES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("queries", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SECONDARY_QUERIES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSecondaryQueries", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSecondaryQueriesScheduled", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    //todo: SERVER_OUT_OF_CAPACITY_EXCEPTIONS is not used anywhere right now. This test case might need to be changed
    // depending on how this metric is used in future
    addMeterWithLables(ServerMeter.SERVER_OUT_OF_CAPACITY_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("serverOutOfCapacityExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("schedulingTimeoutExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("queryExecutionExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.DELETED_SEGMENT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedSegmentCount", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.DELETE_TABLE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deleteTableFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_PARTITION_MISMATCH, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("realtimePartitionMismatch", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REALTIME_DEDUP_DROPPED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("realtimeDedupDropped", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertKeysInWrongSegment", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("partialUpsertKeysNotReplaced", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("partialUpsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedKeysTtlPrimaryKeysRemoved", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("totalKeysMarkedForDeletion", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedKeysWithinTtlWindow", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedTtlKeysInMultipleSegments", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.METADATA_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("metadataTtlPrimaryKeysRemoved", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertMissedValidDocIdSnapshotCount", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.UPSERT_PRELOAD_FAILURE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertPreloadFailure", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_DOCS_SCANNED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numDocsScanned", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numEntriesScannedInFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numEntriesScannedPostFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_QUERIED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsQueried", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PROCESSED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsProcessed", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_MATCHED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsMatched", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_MISSING_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numMissingSegments", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.RELOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("reloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.REFRESH_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("refreshFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("untarFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentStreamedDownloadUntarFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_DIR_MOVEMENT_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDirMovementFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFromRemoteFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFromPeersFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_RESIZES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numResizes", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.RESIZE_TIME_MS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("resizeTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_INVALID, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedInvalid", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedByLimit", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedByValue", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSES_SENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("largeQueryResponsesSent", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("totalThreadCpuTimeMillis", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("largeQueryResponseSizeExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, CLIENT_ID);
    assertMeterExportedCorrectly("rowsWithErrors", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("rowsWithErrors", List.of("table", "myTable_REALTIME_myTopic"),
        EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_FAILURE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadFailure", List.of("table", RAW_TABLE_NAME), EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_SUCCESS, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadSuccess", List.of("table", RAW_TABLE_NAME), EXPORTED_METRIC_PREFIX);

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_TIMEOUT, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadTimeout", List.of("table", RAW_TABLE_NAME), EXPORTED_METRIC_PREFIX);

    //get all exposed metrics now
    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    //We add 25 because 5 metrics (ROWS_WITH_ERRORS, QUERY_EXECUTION_EXCEPTIONS, QUERIES, REALTIME_ROWS_CONSUMED and
    // REALTIME_CONSUMPTION_EXCEPTIONS) are used in 2 different ways in code
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(),
        ServerMeter.values().length * METER_TYPES.size() + 25);
  }

  /**
   * This test validates each meter defined in {@link ServerGauge}
   */
  @Test
  public void serverGaugeTest()
      throws Exception {

    //get all exposed metrics before we expose any timers
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    int partition = 3;
    long someVal = 100L;
    Supplier<Long> someValSupplier = () -> someVal;

    //global gauges
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, someVal);
    _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, someVal);
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, someVal);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_HEAP_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_DIRECT, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_HEAP, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CHUNK_SIZE, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_THREADLOCALCACHE, someValSupplier);

    //local gauges
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.DOCUMENT_COUNT, someVal);
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.SEGMENT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, someVal);

    //raw table name
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, someVal);
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN, someVal);
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, someVal);

    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LLC_PARTITION_CONSUMING, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID,
        ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, someVal);

    _serverMetrics.setValueOfTableGauge(TABLE_STREAM_NAME, ServerGauge.STREAM_DATA_LOSS, 1L);

    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS, someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_DOCS,
        someValSupplier);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
        someValSupplier);

    List<String> labels = List.of("partition", String.valueOf(partition), "table", RAW_TABLE_NAME, "tableType",
        TableType.REALTIME.toString());

    assertGaugeExportedCorrectly("realtimeIngestionOffsetLag", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("upsertPrimaryKeysInSnapshotCount", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("upsertValidDocIdSnapshotCount", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("endToEndRealtimeIngestionDelayMs", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("realtimeIngestionDelayMs", labels, EXPORTED_METRIC_PREFIX);

//    we only match the metric name for version as the label value is dynamic (based on project version).
    Optional<PromMetric> pinotServerVersionMetricMaybe =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse()).stream()
            .filter(exportedMetric -> exportedMetric.getMetricName().contains("version")).findAny();

    Assert.assertTrue(pinotServerVersionMetricMaybe.isPresent());

    assertGaugeExportedCorrectly("llcSimultaneousSegmentBuilds", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedDirectMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledArenasDirect", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledArenasHeap", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledCacheSizeSmall", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledCacheSizeNormal", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledChunkSize", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("jvmHeapUsedBytes", EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("llcPartitionConsuming", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("highestStreamOffsetConsumed", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCreationWaitTimeSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentInitialConsumptionDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCatchupDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(), ServerGauge.values().length);
  }

  public void addGlobalMeter(ServerMeter serverMeter) {
    _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
  }

  public void addMeterWithLables(ServerMeter serverMeter, String label) {
    _serverMetrics.addMeteredTableValue(label, serverMeter, 4L);
  }
}
