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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeOffsetAutoResetManager extends ControllerPeriodicTask<RealtimeOffsetAutoResetManager.Context> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeOffsetAutoResetManager.class);
  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final Map<String, RealtimeOffsetAutoResetHandler> _tableToHandler;
  private final Map<String, Set<String>> _tableTopicsUnderBackfill;

  public RealtimeOffsetAutoResetManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ControllerMetrics controllerMetrics) {
    super("RealtimeOffsetAutoResetManager", config.getRealtimeOffsetAutoResetFrequencyInSeconds(),
        config.getRealtimeOffsetAutoResetInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tableToHandler = new ConcurrentHashMap<>();
    _tableTopicsUnderBackfill = new ConcurrentHashMap<>();
  }

  @Override
  protected RealtimeOffsetAutoResetManager.Context preprocess(Properties periodicTaskProperties) {
    RealtimeOffsetAutoResetManager.Context context = new RealtimeOffsetAutoResetManager.Context();
    // Fill offset back fill job required info
    if (periodicTaskProperties.containsKey(Context.BACKFILL_JOB_PROPERTY_KEYS.toArray()[0])) {
      Context._shouldTriggerBackfillJobs = true;
      for (String key : Context.BACKFILL_JOB_PROPERTY_KEYS) {
        Context._backfillJobProperties.put(key, periodicTaskProperties.getProperty(key));
      }
    }
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, RealtimeOffsetAutoResetManager.Context context) {
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.error("Failed to find table config for table: {}, skipping auto reset periodic job", tableNameWithType);
      return;
    }
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    if (handler == null) {
      return;
    }
    _tableTopicsUnderBackfill.putIfAbsent(tableNameWithType, ConcurrentHashMap.newKeySet());
    String topicName = context._backfillJobProperties.get(Constants.RESET_OFFSET_TOPIC_NAME);
    _tableTopicsUnderBackfill.get(tableNameWithType).add(topicName);

    StreamConfig topicStreamConfig = IngestionConfigUtils.getStreamConfigs(tableConfig).stream()
        .filter(config -> topicName.equals(config.getTopicName()))
        .findFirst().orElseThrow(() -> new RuntimeException("No matching topic found"));

    if (context._shouldTriggerBackfillJobs) {
      _tableToHandler.get(tableNameWithType).triggerBackfillJob(tableNameWithType,
          topicStreamConfig,
          topicName,
          Integer.valueOf(context._backfillJobProperties.get(Constants.RESET_OFFSET_TOPIC_PARTITION)),
          Long.valueOf(context._backfillJobProperties.get(Constants.RESET_OFFSET_FROM)),
          Long.valueOf(context._backfillJobProperties.get(Constants.RESET_OFFSET_TO)));
    }

    ensureBackfillJobsRunning(tableNameWithType);
    ensureCompletedBackfillJobsCleanedUp(tableConfig);
  }

  /**
   * Get the list of tables & topics being backfilled and ensure the backfill jobs are running.
   */
  private void ensureBackfillJobsRunning(String tableNameWithType) {
    // Recover state from ephemeral multi-topics ingestion
    // TODO: refactor or add other recover methods when other backfill approaches are ready
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    for (StreamConfig streamConfig : IngestionConfigUtils.getStreamConfigs(tableConfig)) {
      if (streamConfig.isEphemeralBackfillTopic()) {
        _tableTopicsUnderBackfill.putIfAbsent(tableNameWithType, ConcurrentHashMap.newKeySet());
        _tableTopicsUnderBackfill.get(tableNameWithType).add(streamConfig.getTopicName());
      }
    }
    if (!_tableTopicsUnderBackfill.containsKey(tableNameWithType)) {
      return;
    }
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    if (handler == null) {
      return;
    }
    handler.ensureBackfillJobsRunning(tableNameWithType, _tableTopicsUnderBackfill.get(tableNameWithType));
  }

  private void ensureCompletedBackfillJobsCleanedUp(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    RealtimeOffsetAutoResetHandler handler = getOrConstructHandler(tableConfig);
    Collection<String> cleanedUpTopics = handler.cleanupCompletedBackfillJobs(
        tableNameWithType, _tableTopicsUnderBackfill.get(tableNameWithType));
    if (cleanedUpTopics.size() == _tableTopicsUnderBackfill.get(tableNameWithType).size()) {
      _tableTopicsUnderBackfill.remove(tableNameWithType);
    } else {
      _tableTopicsUnderBackfill.get(tableNameWithType).removeAll(cleanedUpTopics);
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      _tableTopicsUnderBackfill.remove(tableNameWithType);
      _tableToHandler.remove(tableNameWithType);
    }
  }

  private RealtimeOffsetAutoResetHandler getOrConstructHandler(TableConfig tableConfig) {
    RealtimeOffsetAutoResetHandler handler = _tableToHandler.get(tableConfig.getTableName());
    if (handler != null) {
      return handler;
    }
    String className = tableConfig.getIngestionConfig().getStreamIngestionConfig()
        .getRealtimeOffsetAutoResetHandlerClass();
    if (className == null) {
      LOGGER.error("RealtimeOffsetAutoResetHandlerClass is not specified");
      return null;
    }
    try {
      Class<?> clazz = Class.forName(className);
      if (!RealtimeOffsetAutoResetHandler.class.isAssignableFrom(clazz)) {
        String exceptionMessage = "Custom analyzer must be a child of "
            + RealtimeOffsetAutoResetHandler.class.getCanonicalName();
        throw new ReflectiveOperationException(exceptionMessage);
      }

      // Return a new instance of custom lucene analyzer class
      handler = (RealtimeOffsetAutoResetHandler) clazz.getConstructor(
          PinotLLCRealtimeSegmentManager.class, PinotHelixResourceManager.class).newInstance(
              _llcRealtimeSegmentManager, _pinotHelixResourceManager);
      _tableToHandler.put(tableConfig.getTableName(), handler);
      return handler;
    } catch (Exception e) {
      LOGGER.error("Cannot create RealtimeOffsetAutoResetHandler", e);
      return null;
    }
  }



  public static final class Context {
    private static final List<String> BACKFILL_JOB_PROPERTY_KEYS = List.of(
        Constants.RESET_OFFSET_TOPIC_NAME, Constants.RESET_OFFSET_TOPIC_PARTITION,
        Constants.RESET_OFFSET_FROM, Constants.RESET_OFFSET_TO);
    private static boolean _shouldTriggerBackfillJobs;
    private static Map<String, String> _backfillJobProperties = new HashMap<>();
  }
}
