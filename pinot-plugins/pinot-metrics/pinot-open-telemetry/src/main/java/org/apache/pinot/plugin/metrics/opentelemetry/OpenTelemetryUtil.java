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

/**
 * OpenTelemetryUtil provides parsing method to parse pinot metric name (which has dimensions/attributes being
 * concatenated in the metric name) into the Otel metric name and dimensions/attributes.
 */
public class OpenTelemetryUtil {

  private OpenTelemetryUtil() {
    // Utility class, no instantiation
  }

  public static final String OTEL_METRICS_SCOPE = "Pinot";
  public static final String OTEL_ATTRIBUTE_COMPONENT = "Component";
  public static final String OTEL_ATTRIBUTE_RAW_TABLE_NAME = "RawTableName";
  public static final String OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE = "TableNameWithType";
  public static final String OTEL_ATTRIBUTE_TABLE_TYPE = "TableType";
  public static final String OTEL_ATTRIBUTE_TASK_TYPE = "TaskType";
  public static final String OTEL_ATTRIBUTE_TOPIC_NAME = "TopicName";
  public static final String OTEL_ATTRIBUTE_RESOURCE_NAME = "ResourceName";
  public static final String OTEL_ATTRIBUTE_PINOT_METRIC_NAME = "PinotMetricName";

  // pinot.broker.<rawTableName>.<queryPhaseName>
  public static final String[] BROKER_QUERY_PHASE_METRIC_NAME = Stream.of(BrokerQueryPhase.values())
      .map(BrokerQueryPhase::getQueryPhaseName).toArray(String[]::new);

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

  // pinot.controller.<metricName>.<rawTableName>.<topicName>
  public static final String[] CONTROLLER_TOPIC_METRIC = new String[]{
      ControllerGauge.NUM_ROWS_THRESHOLD_WITH_TOPIC.getGaugeName(),
      ControllerGauge.COMMITTING_SEGMENT_SIZE_WITH_TOPIC.getGaugeName()
  };

  // pinot.controller.<resourceName>.<metricName>
  public static final String[] CONTROLLER_IDEAL_STATE_METRIC = new String[]{
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
      String taskType = part.length > 5 ? part[4] : "-";
      dimensions.put(OTEL_ATTRIBUTE_TABLE_NAME_WITH_TYPE, tableNameWithType);
      dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName>.<rawTableName>.<topicName>
    int maybeControllerTopicNameIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_TOPIC_METRIC);
    if (maybeControllerTopicNameIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerTopicNameIndex);
      String rawTableName = part[3];
      dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTableName);
      String topicName = part.length > 5 ? part[4] : "-";
      dimensions.put(OTEL_ATTRIBUTE_TOPIC_NAME, topicName);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<metricName>.<taskType>
    int maybeControllerMinionTaskIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_MINION_TASK_GLOBAL_METRIC);
    if (maybeControllerMinionTaskIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerMinionTaskIndex);
      String taskType = part.length > 4 ? part[3] : "-";
      dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, taskType);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.controller.<resourceName>.<metricName>
    int maybeControllerIdealStateIndex = StringUtils.indexOfAny(pinotMetricName, CONTROLLER_IDEAL_STATE_METRIC);
    if (maybeControllerIdealStateIndex >= 0) {
      otelMetricName = extractOtelMetricName(pinotMetricName, maybeControllerIdealStateIndex);
      String resourceName = part[2];
      dimensions.put(OTEL_ATTRIBUTE_RESOURCE_NAME, resourceName);
      return Pair.of(otelMetricName, dimensions);
    }

    // pinot.<component>.<rawTableName>.<queryPhaseName>
    int mayBeQueryPhaseIndex = StringUtils.indexOfAny(pinotMetricName, BROKER_QUERY_PHASE_METRIC_NAME);
    if (mayBeQueryPhaseIndex >= 0) {
      String rawTableName = part[2];
      dimensions.put(OTEL_ATTRIBUTE_RAW_TABLE_NAME, rawTableName);
      // use the query phase name as the metric name
      otelMetricName = extractOtelMetricName(pinotMetricName, mayBeQueryPhaseIndex);
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
      dimensions.put(OTEL_ATTRIBUTE_TASK_TYPE, tableNameWithType);
      return Pair.of(otelMetricName, dimensions);
    }

    // The remaining Pinot metrics follow the pattern of:
    // pinot.<component>.<metricName>.<rawTableNameOrTableNameWithType>
    otelMetricName = part[2];
    String rawTableNameOrTableNameWithType = part[3];
    String rawTable = "-";
    String tableNameWithType = "-";
    String tableType = "-";
    if (rawTableNameOrTableNameWithType.endsWith("_OFFLINE")) {
      tableNameWithType = rawTableNameOrTableNameWithType;
      tableType = "OFFLINE";
    } else if (rawTableNameOrTableNameWithType.endsWith("_REALTIME")) {
      tableNameWithType = rawTableNameOrTableNameWithType;
      tableType = "REALTIME";
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
