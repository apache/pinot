/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.retention;

import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;


/**
 * RetentionManager is scheduled to run only on Leader controller.
 * It will first scan the table configs to get segment retention strategy then
 * do data retention..
 *
 *
 */
public class RetentionManager {

  public static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _tableDeletionStrategy = new HashMap<String, RetentionStrategy>();
  private final Map<String, List<SegmentZKMetadata>> _segmentMetadataMap = new HashMap<String, List<SegmentZKMetadata>>();
  private final Object _lock = new Object();

  private final ScheduledExecutorService _executorService;
  private final int _runFrequencyInSeconds;

  /**
   * @param pinotHelixResourceManager
   * @param runFrequencyInSeconds
   */
  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _runFrequencyInSeconds = runFrequencyInSeconds;
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotRetentionManagerExecutorService");
        return thread;
      }
    });
  }

  public void start() {
    scheduleRetentionThreadWithFrequency(_runFrequencyInSeconds);
    LOGGER.info("RetentionManager is started!");
  }

  private void scheduleRetentionThreadWithFrequency(int runFrequencyInSeconds) {
    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        synchronized (getLock()) {
          execute();
        }
      }
    }, Math.min(50, runFrequencyInSeconds), runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  private Object getLock() {
    return _lock;
  }

  private void execute() {
    try {
      if (_pinotHelixResourceManager.isLeader()) {
        LOGGER.info("Trying to run retentionManager!");
        updateDeletionStrategiesForEntireCluster();
        LOGGER.info("Finished update deletion strategies for entire cluster!");
        updateSegmentMetadataForEntireCluster();
        LOGGER.info("Finished update segment metadata for entire cluster!");
        scanSegmentMetadataAndPurge();
        LOGGER.info("Finished segment purge for entire cluster!");
      } else {
        LOGGER.info("Not leader of the controller, sleep!");
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while running retention", e);
    }
  }

  private void scanSegmentMetadataAndPurge() {
    for (String tableName : _segmentMetadataMap.keySet()) {
      List<SegmentZKMetadata> segmentZKMetadataList = _segmentMetadataMap.get(tableName);
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        RetentionStrategy deletionStrategy;
        deletionStrategy = _tableDeletionStrategy.get(tableName);
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: {}", segmentZKMetadata.getSegmentName());
          continue;
        }
        if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
          if (((RealtimeSegmentZKMetadata) segmentZKMetadata).getStatus() == Status.IN_PROGRESS) {
            continue;
          }
        }
        if (deletionStrategy.isPurgeable(segmentZKMetadata)) {
          LOGGER.info("Trying to delete segment: {}", segmentZKMetadata.getSegmentName());
          _pinotHelixResourceManager.deleteSegment(tableName, segmentZKMetadata.getSegmentName());
        }
      }
    }
  }

  private void updateDeletionStrategiesForEntireCluster() {
    List<String> tableNames = _pinotHelixResourceManager.getAllPinotTableNames();
    for (String tableName : tableNames) {
      _tableDeletionStrategy.putAll(retrieveDeletionStrategiesForTable(tableName));
    }
  }

  /**
   *
   * @param tableName
   * @return tableName to RetentionStrategy mapping
   */
  private Map<String, RetentionStrategy> retrieveDeletionStrategiesForTable(String tableName) {

    switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
      case OFFLINE:
        return handleOfflineDeletionStrategy(tableName);
      case REALTIME:
        return handleRealtimeDeletionStrategy(tableName);
      default:
        break;
    }
    throw new UnsupportedOperationException("Not support resource type for table name - " + tableName);
  }

  private Map<String, RetentionStrategy> handleOfflineDeletionStrategy(String offlineTableName) {
    Map<String, RetentionStrategy> tableToDeletionStrategyMap = new HashMap<String, RetentionStrategy>();

    try {
      AbstractTableConfig offlineTableConfig;

      try {
        offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);
      } catch (Exception e) {
        LOGGER.error("Error getting offline table config from property store!", e);
        return tableToDeletionStrategyMap;
      }

      if (offlineTableConfig == null || offlineTableConfig.getValidationConfig() == null) {
        LOGGER.info("Table config null for table: {}, treating it as refresh only table.", offlineTableName);
        return tableToDeletionStrategyMap;
      }

      SegmentsValidationAndRetentionConfig validationConfig = offlineTableConfig.getValidationConfig();

      if (validationConfig.getSegmentPushType() == null || validationConfig.getSegmentPushType().isEmpty()) {
        LOGGER.info("Segment push type for table {} is empty, skipping retention processing", offlineTableName);
        return tableToDeletionStrategyMap;
      }

      if ("REFRESH".equalsIgnoreCase(validationConfig.getSegmentPushType())) {
        LOGGER.info("Table: {} is a refresh only table.", offlineTableName);
        return tableToDeletionStrategyMap;
      }

      LOGGER.info("Building retention strategy for table {} with configuration {}", offlineTableName, validationConfig);

      final String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
      final String retentionTimeValue = validationConfig.getRetentionTimeValue();

      if (retentionTimeUnit == null || retentionTimeUnit.isEmpty() || retentionTimeValue == null || retentionTimeValue.isEmpty()) {
        LOGGER.info("Table {} has an invalid retention period of {} {}, skipping", offlineTableName,
            retentionTimeValue, retentionTimeUnit);
        return tableToDeletionStrategyMap;
      }

      TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(retentionTimeUnit,
          retentionTimeValue);
      tableToDeletionStrategyMap.put(offlineTableName, timeRetentionStrategy);
    } catch (Exception e) {
      LOGGER.error("Error creating TimeRetentionStrategy for table: {}", offlineTableName, e);
    }

    return tableToDeletionStrategyMap;
  }

  private Map<String, RetentionStrategy> handleRealtimeDeletionStrategy(String realtimeTableName) {
    Map<String, RetentionStrategy> tableToDeletionStrategyMap = new HashMap<String, RetentionStrategy>();

    AbstractTableConfig realtimeTableConfig;
    try {
      realtimeTableConfig = ZKMetadataProvider.getRealtimeTableConfig(_pinotHelixResourceManager.getPropertyStore(), realtimeTableName);
    } catch (Exception e) {
      LOGGER.error("Error getting realtime table config from property store!", e);
      return tableToDeletionStrategyMap;
    }
    try {
      TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(realtimeTableConfig.getValidationConfig().getRetentionTimeUnit(),
          realtimeTableConfig.getValidationConfig().getRetentionTimeValue());
      tableToDeletionStrategyMap.put(realtimeTableName, timeRetentionStrategy);
    } catch (Exception e) {
      LOGGER.error("Error creating TimeRetentionStrategy for table {}", realtimeTableName, e);
    }
    return tableToDeletionStrategyMap;
  }

  private void updateSegmentMetadataForEntireCluster() {
    List<String> tableNames = _pinotHelixResourceManager.getAllPinotTableNames();
    for (String tableName : tableNames) {
      _segmentMetadataMap.put(tableName, retrieveSegmentMetadataForTable(tableName));
    }
  }

  private List<SegmentZKMetadata> retrieveSegmentMetadataForTable(String tableName) {
    List<SegmentZKMetadata> segmentMetadataList = new ArrayList<SegmentZKMetadata>();
    switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
      case OFFLINE:
        List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas =
            ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(), tableName);
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
          segmentMetadataList.add(offlineSegmentZKMetadata);
        }
        break;
      case REALTIME:
        List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadatas =
            ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(), tableName);
        for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : realtimeSegmentZKMetadatas) {
          segmentMetadataList.add(realtimeSegmentZKMetadata);
        }
        break;
      default:
        break;
    }
    return segmentMetadataList;
  }

  public void stop() {
    _executorService.shutdown();

  }

}
