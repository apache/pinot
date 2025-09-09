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
package org.apache.pinot.plugin.metrics.opentelemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.metrics.MinionGauge;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionTimer;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerQueryPhase;


/**
 * OpenTelemetryUtil provides parsing method to parse pinot metric name (which has dimensions/attributes being
 * concatenated in the metric name) into the Otel metric name and dimensions/attributes.
 */
public class OpenTelemetryUtil {

  private OpenTelemetryUtil() {
    // Utility class, no instantiation
  }

  public static Attributes toOpenTelemetryAttributes(Map<String, String> attributes) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    if (attributes != null) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        attributesBuilder.put(entry.getKey(), entry.getValue() == null ? "-" : entry.getValue());
      }
    }
    return attributesBuilder.build();
  }

  public static final String TABLE_TYPE_OFFLINE = "OFFLINE";
  public static final String TABLE_TYPE_REALTIME = "REALTIME";
  public static final String TABLE_TYPE_OFFLINE_SUFFIX = "_OFFLINE";
  public static final String TABLE_TYPE_REALTIME_SUFFIX = "_REALTIME";

  public static final String OTEL_METRICS_SCOPE = "Pinot";

  public static final String OTEL_ATTRIBUTE_COMPONENT = "Component";
  public static final String OTEL_ATTRIBUTE_RAW_TABLE_NAME = "RawTableName";
  public static final String OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE = "TableNameWithType";
  public static final String OTEL_ATTRIBUTE_TABLE_TYPE = "TableType";
  public static final String OTEL_ATTRIBUTE_TASK_TYPE = "TaskType";
  public static final String OTEL_ATTRIBUTE_TOPIC_NAME = "TopicName";
  public static final String OTEL_ATTRIBUTE_CONSUMER_CLIENT_ID = "ClientId";
  public static final String OTEL_ATTRIBUTE_CONSUMER_PARTITION_ID = "PartitionId";
  public static final String OTEL_ATTRIBUTE_JSON_COLUMN_NAME = "ColumnName";
  public static final String OTEL_ATTRIBUTE_RESOURCE_NAME = "ResourceName";
  public static final String OTEL_ATTRIBUTE_PINOT_METRIC_NAME = "PinotMetricName";

  public static final String OTEL_MISSING_DIMENSION_VALUE = "-";

  // pinot.broker.<rawTableName>.<phaseName>
  public static final String[] BROKER_QUERY_PHASE_METRIC_NAME = Stream.of(BrokerQueryPhase.values())
      .map(BrokerQueryPhase::getQueryPhaseName).toArray(String[]::new);
  // pinot.server.<tableNameWithType>.<phaseName>
  public static final String[] SERVER_PHASE_METRIC_NAME = Stream.of(ServerQueryPhase.values())
      .map(ServerQueryPhase::getQueryPhaseName).toArray(String[]::new);

  // pinot.controller.<metricName>.<taskType>
  public static final String[] CONTROLLER_MINION_TASK_GLOBAL_METRIC = new String[]{
      ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_WAITING.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_RUNNING.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_ERROR.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_DROPPED.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT.getGaugeName(),
      ControllerGauge.NUM_MINION_SUBTASKS_ABORTED.getGaugeName()
  };

  // pinot.controller.<metricName><tableNameWithType>.<taskType>
  public static final String[] CONTROLLER_CRON_JOB_METRIC = new String[]{
      ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED.getGaugeName(),

      ControllerMeter.CRON_SCHEDULER_JOB_TRIGGERED.getMeterName(),
      ControllerMeter.CRON_SCHEDULER_JOB_SKIPPED.getMeterName(),

      ControllerTimer.CRON_SCHEDULER_JOB_EXECUTION_TIME_MS.getTimerName()
  };

  // pinot.controller.<rawTableName>.<metricName>
  public static final String[] CONTROLLER_DEEP_STORE_METRIC = new String[]{
      ControllerTimer.DEEP_STORE_SEGMENT_READ_TIME_MS.getTimerName(),
      ControllerTimer.DEEP_STORE_SEGMENT_WRITE_TIME_MS.getTimerName()
  };

  // pinot.controller.<metricName>.<rawTableName>.<topicName>
  public static final String[] CONTROLLER_TOPIC_METRIC = new String[]{
      ControllerGauge.NUM_ROWS_THRESHOLD_WITH_TOPIC.getGaugeName(),
      ControllerGauge.COMMITTING_SEGMENT_SIZE_WITH_TOPIC.getGaugeName()
  };

  // pinot.controller.<resourceName>.<metricName>
  public static final String[] CONTROLLER_IDEAL_STATE_UPDATE_METRIC = new String[]{
      ControllerMeter.IDEAL_STATE_UPDATE_FAILURE.getMeterName(),
      ControllerMeter.IDEAL_STATE_UPDATE_RETRY.getMeterName(),
      ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS.getMeterName(),

      ControllerTimer.IDEAL_STATE_UPDATE_TIME_MS.getTimerName()
  };

  // pinot.minion.<tableName>.<taskType>.<metricName> or pinot.minion.<taskType>.<metricName>
  public static final String[] MINION_TASK_METRIC = new String[]{
      MinionGauge.NUMBER_OF_TASKS.getGaugeName(),

      MinionMeter.NUMBER_TASKS.getMeterName(),
      MinionMeter.NUMBER_TASKS_EXECUTED.getMeterName(),
      MinionMeter.NUMBER_TASKS_COMPLETED.getMeterName(),
      MinionMeter.NUMBER_TASKS_CANCELLED.getMeterName(),
      MinionMeter.NUMBER_TASKS_FAILED.getMeterName(),
      MinionMeter.NUMBER_TASKS_FATAL_FAILED.getMeterName(),
      MinionMeter.SEGMENT_UPLOAD_FAIL_COUNT.getMeterName(),
      MinionMeter.SEGMENT_DOWNLOAD_FAIL_COUNT.getMeterName(),
      MinionMeter.SEGMENT_DOWNLOAD_COUNT.getMeterName(),
      MinionMeter.SEGMENT_UPLOAD_COUNT.getMeterName(),
      MinionMeter.SEGMENT_BYTES_DOWNLOADED.getMeterName(),
      MinionMeter.SEGMENT_BYTES_UPLOADED.getMeterName(),
      MinionMeter.RECORDS_PROCESSED_COUNT.getMeterName(),
      MinionMeter.RECORDS_PURGED_COUNT.getMeterName(),

      MinionTimer.TASK_EXECUTION.getTimerName(),
      MinionTimer.TASK_QUEUEING.getTimerName(),
      MinionTimer.TASK_THREAD_CPU_TIME_NS.getTimerName()
  };

  // pinot.minion.<tableName>.<metricName>
  public static final String[] MINION_SEGMENT_CREATION_METRIC = new String[]{
      MinionMeter.COMPACTED_RECORDS_COUNT.getMeterName(),
      MinionMeter.TRANSFORMATION_ERROR_COUNT.getMeterName(),
      MinionMeter.DROPPED_RECORD_COUNT.getMeterName(),
      MinionMeter.CORRUPTED_RECORD_COUNT.getMeterName()
  };

  // pinot.server.<metricName>.<clientId> where
  // clientId = _tableNameWithType + "-" + streamTopic + "-" + _streamPartitionId + "-" + clientIdSuffix;
  public static final String[] LLC_PARTITION_CONSUMING_METRIC = new String[]{
      ServerGauge.LLC_PARTITION_CONSUMING.getGaugeName(),
      ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED.getGaugeName(),
      ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS.getGaugeName(),
      ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS.getGaugeName(),
      ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS.getGaugeName(),
      ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS.getGaugeName(),
      ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS.getGaugeName(),
      ServerGauge.PAUSELESS_CONSUMPTION_ENABLED.getGaugeName(),
      ServerGauge.CONSUMPTION_QUOTA_UTILIZATION.getGaugeName(),
      ServerGauge.STREAM_DATA_LOSS.getGaugeName(),

      ServerMeter.REALTIME_ROWS_FETCHED.getMeterName(),
      ServerMeter.REALTIME_ROWS_FILTERED.getMeterName(),
      ServerMeter.INVALID_REALTIME_ROWS_DROPPED.getMeterName(),
      ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED.getMeterName(),
      ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS.getMeterName(),
      ServerMeter.ROWS_WITH_ERRORS.getMeterName()
  };

  // pinot.server.<metricName>.<tableNameWithType>.<partitionId>
  public static final String[] REALTIME_CONSUMPTION_PARTITION_METRIC = new String[]{
      ServerGauge.UPSERT_PRIMARY_KEYS_COUNT.getGaugeName(),
      ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT.getGaugeName(),
      ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT.getGaugeName(),
      ServerGauge.REALTIME_INGESTION_OFFSET_LAG.getGaugeName(),
      ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET.getGaugeName(),
      ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET.getGaugeName(),
      ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS.getGaugeName(),
      ServerGauge.DEDUP_PRIMARY_KEYS_COUNT.getGaugeName(),
      ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName(),

      ServerGauge.LUCENE_INDEXING_DELAY_MS.getGaugeName(),
      ServerGauge.LUCENE_INDEXING_DELAY_DOCS.getGaugeName()
  };

  // pinot.server.<metricName>.<tableNameWithType>.<columnName>
  public static final String[] JSON_INDEX_METRIC = new String[]{
      ServerMeter.MUTABLE_JSON_INDEX_MEMORY_USAGE.getMeterName(),
  };


  static public Pair<String, Map<String, String>> parseOtelMetricNameAndDimensions(String pinotMetricName) {
    String[] part = pinotMetricName.split("\\.");
    // tentatively use the pinotMetricName metric name as the otel metric name, so that if the parsing failed to handle
    // some corner cases, we can still use the legacy metric name as the otel metric name
    String otelMetricName = pinotMetricName;
    Map<String, String> dimensions = new HashMap<>();
    // put the pinot metric name as one of Otel metric dimension, so that we can still trace back to the original metric
    // name, this is useful for debugging and fixing the dimension parsing logic in case of the Otel metric name is not
    // what we expect.
    dimensions.put(OTEL_ATTRIBUTE_PINOT_METRIC_NAME, pinotMetricName);

    // should never happen though
    if (part.length < 3) {
      return Pair.of(otelMetricName, dimensions);
    }
    dimensions.put(OTEL_ATTRIBUTE_COMPONENT, part[1]);

    // this is a component level global metric, e.g. pinot.broker.nettyConnection
    if (part.length < 4) {
      otelMetricName = part[2];
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName><tableNameWithType>.<taskType>
    int maybeControllerCronJobIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_CRON_JOB_METRIC);
    if (maybeControllerCronJobIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerCronJobIndex);
      String tableNameWithType = part[3];
      String taskType = part.length > 5 ? part[4] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName>.<rawTableName>.<topicName>
    int maybeControllerTopicNameIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_TOPIC_METRIC);
    if (maybeControllerTopicNameIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerTopicNameIndex);
      String rawTableName = part[3];
      String topicName = part.length > 5 ? part[4] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTableName);
      dimensions.put(OTEL_ATTRIBUTE_TOPIC_NAME, topicName);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName>.<taskType>
    int maybeControllerMinionTaskIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_MINION_TASK_GLOBAL_METRIC);
    if (maybeControllerMinionTaskIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerMinionTaskIndex);
      String taskType = part.length > 4 ? part[3] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<resourceName>.<metricName>
    int maybeControllerIdealStateUpdateIndex = StringUtils.indexOfAny(pinotMetricName,
        CONTROLLER_IDEAL_STATE_UPDATE_METRIC);
    if (maybeControllerIdealStateUpdateIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerIdealStateUpdateIndex);
      String resourceName = part[2];

      dimensions.put(OTEL_ATTRIBUTE_RESOURCE_NAME, resourceName);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName>.<rawTableName>
    int maybeControllerDeepStoreIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_DEEP_STORE_METRIC);
    if (maybeControllerDeepStoreIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerDeepStoreIndex);
      String rawTableName = part[3];

      dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTableName);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.broker.<rawTableName>.<queryPhaseName>
    int maybeBrokerQueryPhaseIndex = StringUtils.indexOfAny(pinotMetricName, BROKER_QUERY_PHASE_METRIC_NAME);
    if (maybeBrokerQueryPhaseIndex >= 0) {
      String rawTableName = part[2];

      dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTableName);
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeBrokerQueryPhaseIndex);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.server.<tableNameWithType>.<queryPhaseName>
    int maybeServerQueryPhaseIndex = StringUtils.indexOfAny(pinotMetricName, SERVER_PHASE_METRIC_NAME);
    if (maybeServerQueryPhaseIndex >= 0) {
      String tableNameWithType = part[2];

      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeServerQueryPhaseIndex);
      return Pair.of(otelMetricName, dimensions);
    }


    // pinot.minion.<tableName>.<taskType>.<metricName> or pinot.minion.<taskType>.<metricName>
    int maybeMinionTaskIndex = StringUtils.indexOfAny(pinotMetricName, MINION_TASK_METRIC);
    if (maybeMinionTaskIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeMinionTaskIndex);
      if (Arrays.asList(MINION_TASK_METRIC).indexOf(otelMetricName) == 3) {
        String taskType = part[2];

        dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
        return Pair.of(otelMetricName, dimensions);
      } else {
        String tableNameWithType = part[2];
        String taskType = part[3];

        dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
        dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
        return Pair.of(otelMetricName, dimensions);
      }
    }

    // pinot.minion.<tableName>.<metricName>
    int maybeMinionSegmentCreationIndex = StringUtils.indexOfAny(pinotMetricName, MINION_SEGMENT_CREATION_METRIC);
    if (maybeMinionSegmentCreationIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeMinionSegmentCreationIndex);
      String tableNameWithType = part[2];

      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.server.<metricName>.<clientId> where
    // clientId = _tableNameWithType + "-" + streamTopic + "-" + _streamPartitionId + "-" + clientIdSuffix;
    int maybeLlcConsumingIndex = StringUtils.indexOfAny(pinotMetricName, LLC_PARTITION_CONSUMING_METRIC);
    if (maybeLlcConsumingIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeLlcConsumingIndex);
      String clientId = part[2];

      String[] clientIdParts = clientId.split("-");
      String tableNameWithType = clientIdParts.length > 0 ? clientIdParts[0] : OTEL_MISSING_DIMENSION_VALUE;
      String topicName = clientIdParts.length > 1 ? clientIdParts[1] : OTEL_MISSING_DIMENSION_VALUE;
      String partitionId = clientIdParts.length > 2 ? clientIdParts[2] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_CONSUMER_CLIENT_ID, clientId);
      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      dimensions.put(OTEL_ATTRIBUTE_TOPIC_NAME, topicName);
      dimensions.put(OTEL_ATTRIBUTE_CONSUMER_PARTITION_ID, partitionId);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.server.<metricName>.<tableNameWithType>.<partitionId>
    int maybeRealtimeConsumptionPartitionIndex =
        StringUtils.indexOfAny(pinotMetricName, REALTIME_CONSUMPTION_PARTITION_METRIC);
    if (maybeRealtimeConsumptionPartitionIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeRealtimeConsumptionPartitionIndex);
      String tableNameWithType = part[2];
      String partitionId = part.length > 4 ? part[3] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      dimensions.put(OTEL_ATTRIBUTE_CONSUMER_PARTITION_ID, partitionId);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.server.<metricName>.<tableNameWithType>.<columnName>
    int maybeJsonIndexMetric = StringUtils.indexOfAny(pinotMetricName, JSON_INDEX_METRIC);
    if (maybeJsonIndexMetric >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeJsonIndexMetric);
      String tableNameWithType = part[2];
      String columnName = part.length > 4 ? part[3] : OTEL_MISSING_DIMENSION_VALUE;

      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      dimensions.put(OTEL_ATTRIBUTE_JSON_COLUMN_NAME, columnName);
      return Pair.of(otelMetricName, dimensions);
    }

    // Remaining Pinot metrics follow the pattern of:
    // if it's a Pinot Gauge: pinot.<component>.<metricName>.<tableNameWithTypeOrRawTableName>
    // if it's a Pinot Timer or Pinot Meter: pinot.<component>.<tableNameWithTypeOrRawTableName>.<metricName>
    otelMetricName = part[2];
    String rawTableNameOrTableNameWithType = part[3];
    String tableNameWithType = OTEL_MISSING_DIMENSION_VALUE;
    String rawTable = OTEL_MISSING_DIMENSION_VALUE;
    String tableType = OTEL_MISSING_DIMENSION_VALUE;
    if (rawTableNameOrTableNameWithType.endsWith(TABLE_TYPE_OFFLINE_SUFFIX)) {
      tableNameWithType = rawTableNameOrTableNameWithType;
      rawTable = rawTableNameOrTableNameWithType.substring(0,
          rawTableNameOrTableNameWithType.length() - TABLE_TYPE_OFFLINE_SUFFIX.length());
      tableType = TABLE_TYPE_OFFLINE;
    } else if (rawTableNameOrTableNameWithType.endsWith(TABLE_TYPE_REALTIME_SUFFIX)) {
      tableNameWithType = rawTableNameOrTableNameWithType;
      rawTable = rawTableNameOrTableNameWithType.substring(0,
          rawTableNameOrTableNameWithType.length() - TABLE_TYPE_REALTIME_SUFFIX.length());
      tableType = TABLE_TYPE_REALTIME;
    } else {
      rawTable = rawTableNameOrTableNameWithType;
    }

    dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTable);
    dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
    dimensions.put(OTEL_ATTRIBUTE_TABLE_TYPE, tableType);
    return Pair.of(otelMetricName, dimensions);
  }

  private static String extractOtelMetricName(String pinotMetricName, int startIndex) {
    int endIndex = pinotMetricName.indexOf('.', startIndex);
    return endIndex == -1 ? pinotMetricName.substring(startIndex) : pinotMetricName.substring(startIndex, endIndex);
  }
}
