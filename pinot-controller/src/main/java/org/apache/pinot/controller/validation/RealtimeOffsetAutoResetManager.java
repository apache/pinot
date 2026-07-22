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
package org.apache.pinot.controller.validation;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.periodictask.RealtimeOffsetAutoResetHandler;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeOffsetAutoResetManager extends ControllerPeriodicTask<RealtimeOffsetAutoResetManager.Context> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeOffsetAutoResetManager.class);
  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerConf _controllerConf;
  private final Map<String, RealtimeOffsetAutoResetHandler> _tableToHandler;
  private final Map<String, Set<String>> _sourceTopicsByTable;
  private final Map<String, Set<String>> _backfillTopicsByTable;
  // Key: "tableNameWithType:topicName:partitionId" — counts how many times this partition triggered
  // a backfill while a prior one for that same partition was still in flight.
  private final Map<String, Integer> _partitionsInFlightCount;

  public RealtimeOffsetAutoResetManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ControllerMetrics controllerMetrics) {
    super("RealtimeOffsetAutoResetManager", config.getRealtimeOffsetAutoResetBackfillFrequencyInSeconds(),
            config.getRealtimeOffsetAutoResetBackfillInitialDelaySeconds(),
        config.getRealtimeOffsetAutoResetBackfillCronExpression(),
        pinotHelixResourceManager, leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = config;
    _tableToHandler = new ConcurrentHashMap<>();
    _sourceTopicsByTable = new ConcurrentHashMap<>();
    _backfillTopicsByTable = new ConcurrentHashMap<>();
    _partitionsInFlightCount = new ConcurrentHashMap<>();
  }

  @Override
  protected RealtimeOffsetAutoResetManager.Context preprocess(Properties periodicTaskProperties) {
    RealtimeOffsetAutoResetManager.Context context = new RealtimeOffsetAutoResetManager.Context();
    // Fill offset backfill job required info
    // Examples of properties:
    //   resetOffsetTopicName=topicName
    //   resetOffsetTopicPartition=0
    //   resetOffsetFrom=0
    //   resetOffsetTo=1000
    if (periodicTaskProperties.keySet().containsAll(context._backfillJobPropertyKeys)) {
      context._shouldTriggerBackfillJobs = true;
      for (String key : context._backfillJobPropertyKeys) {
        context._backfillJobProperties.put(key, periodicTaskProperties.getProperty(key));
      }
    }
    return context;
  }

  @VisibleForTesting
  protected RealtimeOffsetAutoResetHandler getTableHandler(String tableNameWithType) {
    return _tableToHandler.get(tableNameWithType);
  }

  @Override
  protected void processTable(String tableNameWithType, RealtimeOffsetAutoResetManager.Context context) {
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }
    LOGGER.info("Processing offset auto reset backfill for table {}, with context {}", tableNameWithType, context);

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.error("Failed to find table config for table: {}, skipping auto reset periodic job", tableNameWithType);
      return;
    }
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    if (handler == null) {
      return;
    }

    // Skip triggering if the controller is already handling the maximum number of concurrent backfills
    int maxConcurrent = _controllerConf.getMaxConcurrentBackfillsPerController();
    if (context._shouldTriggerBackfillJobs && maxConcurrent > 0
        && _backfillTopicsByTable.size() >= maxConcurrent) {
      LOGGER.warn("Skipping backfill trigger for table {} — max concurrent backfills ({}) reached",
          tableNameWithType, maxConcurrent);
      _controllerMetrics.addMeteredTableValue(tableNameWithType,
          ControllerMeter.OFFSET_AUTO_RESET_BACKFILL_SKIPPED_MAX_CONCURRENT, 1L);
      context._shouldTriggerBackfillJobs = false;
    }

    if (context._shouldTriggerBackfillJobs) {
      _sourceTopicsByTable.putIfAbsent(tableNameWithType, ConcurrentHashMap.newKeySet());
      String topicName = context._backfillJobProperties.get(Constants.RESET_OFFSET_TOPIC_NAME);
      String partitionStr = context._backfillJobProperties.get(Constants.RESET_OFFSET_TOPIC_PARTITION);
      String metricKey = topicName + "." + partitionStr;
      _sourceTopicsByTable.get(tableNameWithType).add(topicName);

      // Per-partition in-flight guard: if this partition previously triggered a backfill that is
      // still running, increment its collision counter. Auto-pause when the threshold is exceeded.
      String partitionKey = tableNameWithType + ":" + topicName + ":" + partitionStr;
      if (_partitionsInFlightCount.containsKey(partitionKey)) {
        int collisions = _partitionsInFlightCount.merge(partitionKey, 1, Integer::sum);
        int maxCollisions = _controllerConf.getMaxBackfillCollisionsBeforeAutoPause();
        _controllerMetrics.addMeteredTableValue(tableNameWithType, metricKey,
            ControllerMeter.OFFSET_AUTO_RESET_BACKFILL_SKIPPED_IN_FLIGHT, 1L);
        LOGGER.warn("In-flight backfill collision #{} for partition key {}", collisions, partitionKey);
        if (maxCollisions > 0 && collisions >= maxCollisions) {
          LOGGER.warn("Auto-pausing backfill for table {} topic {} partition {} after {} collisions",
              tableNameWithType, topicName, partitionStr, collisions);
          _controllerMetrics.addMeteredTableValue(tableNameWithType, metricKey,
              ControllerMeter.OFFSET_AUTO_RESET_BACKFILL_SKIPPED_PAUSED, 1L);
          setPauseFlag(tableNameWithType, tableConfig, topicName);
          context._shouldTriggerBackfillJobs = false;
        }
        // Below threshold: allow trigger to proceed (new backfill coexists with the ongoing one)
      }

      StreamConfig topicStreamConfig = IngestionConfigUtils.getStreamConfigs(tableConfig).stream()
          .filter(config -> topicName.equals(config.getTopicName()))
          .findFirst().orElseThrow(() -> new RuntimeException("No matching topic found"));
      if (context._shouldTriggerBackfillJobs) {
        LOGGER.info("Triggering backfill jobs with StreamConfig {}, topicName {}, properties {}",
            topicStreamConfig, topicName, context._backfillJobProperties);
        try {
          long startOffset = Long.parseLong(context._backfillJobProperties.get(Constants.RESET_OFFSET_FROM));
          long endOffset = Long.parseLong(context._backfillJobProperties.get(Constants.RESET_OFFSET_TO));
          if (_tableToHandler.get(tableNameWithType).triggerBackfillJob(tableNameWithType,
              topicStreamConfig,
              topicName,
              Integer.parseInt(partitionStr),
              startOffset,
              endOffset)) {
            // Mark this partition as having an in-flight backfill (collision count starts at 1)
            _partitionsInFlightCount.put(partitionKey, 1);
            _controllerMetrics.addMeteredTableValue(tableNameWithType, metricKey,
                ControllerMeter.OFFSET_AUTO_RESET_BACKFILL_OFFSETS, endOffset - startOffset);
          }
        } catch (NumberFormatException e) {
          LOGGER.error("Invalid backfill job properties for table: {}, properties: {}, error: {}",
              tableNameWithType, context._backfillJobProperties, e.getMessage(), e);
        }
      }
    }

    ensureBackfillJobsRunning(tableNameWithType);
    ensureCompletedBackfillJobsCleanedUp(tableConfig);

    Set<String> activeTopics = _backfillTopicsByTable.get(tableNameWithType);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType,
        ControllerGauge.BACKFILL_TOPICS_IN_PROGRESS,
        activeTopics != null ? activeTopics.size() : 0L);
  }

  /**
   * Get the list of tables & topics being backfilled and ensure the backfill jobs are running.
   */
  private void ensureBackfillJobsRunning(String tableNameWithType) {
    // Recover state from ephemeral multi-topics ingestion
    // TODO: refactor or add other recover methods when other backfill approaches are ready
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(tableConfig);
    for (int i = 0; i < streamConfigs.size(); i++) {
      if (streamConfigs.get(i).isBackfillTopic() && !_llcRealtimeSegmentManager.isTopicConsumptionPaused(
          tableConfig.getTableName(), i)) {
        _backfillTopicsByTable.putIfAbsent(tableNameWithType, ConcurrentHashMap.newKeySet());
        _backfillTopicsByTable.get(tableNameWithType).add(streamConfigs.get(i).getTopicName());
      }
    }
    if (!_sourceTopicsByTable.containsKey(tableNameWithType)
        || _sourceTopicsByTable.get(tableNameWithType).isEmpty()) {
      return;
    }
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    if (handler == null) {
      return;
    }
    handler.ensureBackfillJobsRunning(tableNameWithType, _sourceTopicsByTable.get(tableNameWithType));
  }

  private void ensureCompletedBackfillJobsCleanedUp(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    if (!_backfillTopicsByTable.containsKey(tableNameWithType)) {
      return;
    }
    LOGGER.info("Trying to clean up backfill jobs on {}", tableNameWithType);
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    Collection<String> cleanedUpTopics = handler.cleanupCompletedBackfillJobs(
        tableNameWithType, _backfillTopicsByTable.get(tableNameWithType));
    if (cleanedUpTopics.containsAll(_backfillTopicsByTable.get(tableNameWithType))) {
      _sourceTopicsByTable.remove(tableNameWithType);
      _backfillTopicsByTable.remove(tableNameWithType);
      // Remove all per-partition collision counters for this table
      _partitionsInFlightCount.keySet().removeIf(k -> k.startsWith(tableNameWithType + ":"));
      if (_tableToHandler.get(tableNameWithType) != null) {
        _tableToHandler.get(tableNameWithType).close();
        _tableToHandler.remove(tableNameWithType);
      }
    } else {
      _backfillTopicsByTable.get(tableNameWithType).removeAll(cleanedUpTopics);
    }
    if (cleanedUpTopics.size() > 0) {
      LOGGER.info("Cleaned up complete backfill topics {} for table {}", cleanedUpTopics, tableNameWithType);
      _controllerMetrics.addMeteredTableValue(tableNameWithType,
          ControllerMeter.OFFSET_AUTO_RESET_BACKFILL_CLEANUP_COMPLETED, cleanedUpTopics.size());
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      _sourceTopicsByTable.remove(tableNameWithType);
      _backfillTopicsByTable.remove(tableNameWithType);
      _tableToHandler.remove(tableNameWithType);
      _partitionsInFlightCount.keySet().removeIf(k -> k.startsWith(tableNameWithType + ":"));
    }
  }

  private void setPauseFlag(String tableNameWithType, TableConfig tableConfig, String topicName) {
    List<Map<String, String>> streamConfigMaps =
        tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
    for (Map<String, String> map : streamConfigMaps) {
      // Topic name is stored under the prefixed key "stream.<type>.topic.name"
      String streamType = map.get(StreamConfigProperties.STREAM_TYPE);
      String topicKey =
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME);
      if (topicName.equals(map.get(topicKey))) {
        map.put(StreamConfigProperties.OFFSET_AUTO_RESET_PAUSE, "true");
        break;
      }
    }
    try {
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
      LOGGER.info("Set offset auto reset pause flag for table {} topic {}", tableNameWithType, topicName);
    } catch (Exception e) {
      LOGGER.error("Failed to set pause flag for table {} topic {}", tableNameWithType, topicName, e);
      _controllerMetrics.addMeteredTableValue(tableNameWithType,
          ControllerMeter.OFFSET_AUTO_RESET_AUTO_PAUSE_FAILURE, 1L);
    }
  }

  private RealtimeOffsetAutoResetHandler getOrConstructHandler(TableConfig tableConfig) {
    RealtimeOffsetAutoResetHandler handler = _tableToHandler.get(tableConfig.getTableName());
    if (handler != null) {
      return handler;
    }
    if (tableConfig.getIngestionConfig() == null
        || tableConfig.getIngestionConfig().getStreamIngestionConfig() == null) {
      LOGGER.debug("Table {} config is in the legacy mode, cannot do auto reset", tableConfig.getTableName());
      return null;
    }
    String className = tableConfig.getIngestionConfig().getStreamIngestionConfig()
        .getRealtimeOffsetAutoResetHandlerClass();
    if (className == null) {
      LOGGER.debug("RealtimeOffsetAutoResetHandlerClass is not specified for table {}", tableConfig.getTableName());
      return null;
    }
    try {
      Class<?> clazz = Class.forName(className);
      if (!RealtimeOffsetAutoResetHandler.class.isAssignableFrom(clazz)) {
        throw new ReflectiveOperationException("Custom handler must implement "
            + RealtimeOffsetAutoResetHandler.class.getCanonicalName());
      }
      try {
        // Preferred: no-arg constructor + explicit init()
        handler = (RealtimeOffsetAutoResetHandler) clazz.getConstructor().newInstance();
        handler.init(_llcRealtimeSegmentManager, _pinotHelixResourceManager);
      } catch (NoSuchMethodException e) {
        // Backward-compatibility fallback: 2-arg constructor (deprecated SPI contract).
        // Handlers compiled against the previous contract called init() from their own constructor.
        LOGGER.warn("Handler class {} has no no-arg constructor; falling back to deprecated 2-arg constructor. "
            + "Please migrate to a no-arg constructor + init() pattern.", className);
        handler = (RealtimeOffsetAutoResetHandler) clazz.getConstructor(
            PinotLLCRealtimeSegmentManager.class, PinotHelixResourceManager.class)
            .newInstance(_llcRealtimeSegmentManager, _pinotHelixResourceManager);
      }
      _tableToHandler.put(tableConfig.getTableName(), handler);
      return handler;
    } catch (Exception e) {
      LOGGER.error("Cannot create RealtimeOffsetAutoResetHandler", e);
      _controllerMetrics.addMeteredTableValue(tableConfig.getTableName(),
          ControllerMeter.OFFSET_AUTO_RESET_HANDLER_INIT_FAILURE, 1L);
      return null;
    }
  }

  public class Context {
    public final List<String> _backfillJobPropertyKeys = List.of(
        Constants.RESET_OFFSET_TOPIC_NAME, Constants.RESET_OFFSET_TOPIC_PARTITION,
        Constants.RESET_OFFSET_FROM, Constants.RESET_OFFSET_TO);
    private boolean _shouldTriggerBackfillJobs;
    private Map<String, String> _backfillJobProperties = new HashMap<>();

    @VisibleForTesting
    protected boolean isShouldTriggerBackfillJobs() {
      return _shouldTriggerBackfillJobs;
    }

    @VisibleForTesting
    protected Map<String, String> getBackfillJobProperties() {
      return _backfillJobProperties;
    }

    @Override
    public String toString() {
      return _backfillJobProperties.toString();
    }
  }
}
