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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;


/**
 * RetentionManager is scheduled to run only on Leader controller.
 * It will first scan the resource configs to get segment retention strategy then
 * do data retention..
 *
 * @author xiafu
 *
 */
public class RetentionManager {

  public static final Logger LOGGER = Logger.getLogger(RetentionManager.class);

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _tableDeletionStrategy = new HashMap<String, RetentionStrategy>();
  private final Map<String, List<SegmentZKMetadata>> _segmentMetadataMap = new HashMap<String, List<SegmentZKMetadata>>();
  private final Object _lock = new Object();

  private final ScheduledExecutorService _executorService;
  private final int _runFrequencyInSeconds;

  private final static String REFRESH = "REFRESH";

  /**
   * @param pinotHelixResourceManager
   * @param runFrequencyInSeconds
   */
  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _propertyStore = pinotHelixResourceManager.getPropertyStore();
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
    }, 5, runFrequencyInSeconds, TimeUnit.SECONDS);
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
      LOGGER.error("Got error in deletion thread: " + e);
    }
  }

  private void scanSegmentMetadataAndPurge() {
    for (String resourceName : _segmentMetadataMap.keySet()) {
      List<SegmentZKMetadata> segmentZKMetadataList = _segmentMetadataMap.get(resourceName);
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        RetentionStrategy deletionStrategy;
        if (_tableDeletionStrategy.containsKey(resourceName + "." + segmentZKMetadata.getTableName())) {
          deletionStrategy = _tableDeletionStrategy.get(resourceName + "." + segmentZKMetadata.getTableName());
        } else {
          deletionStrategy = _tableDeletionStrategy.get(resourceName + ".*");
        }
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: " + segmentZKMetadata.getSegmentName());
          continue;
        }
        if (deletionStrategy.isPurgeable(segmentZKMetadata)) {
          LOGGER.info("Trying to delete segment: " + segmentZKMetadata.getSegmentName());
          _pinotHelixResourceManager.deleteSegment(resourceName, segmentZKMetadata.getSegmentName());
        }
      }
    }
  }

  private void updateDeletionStrategiesForEntireCluster() {
    List<String> resourceNames = _pinotHelixResourceManager.getAllPinotResourceNames();
    for (String resourceName : resourceNames) {
      _tableDeletionStrategy.putAll(retrieveDeletionStrategiesForResource(resourceName));
    }
  }

  /**
   *
   * @param resourceName
   * @return tableName to RetentionStrategy mapping
   */
  private Map<String, RetentionStrategy> retrieveDeletionStrategiesForResource(String resourceName) {
    Map<String, RetentionStrategy> tableToDeletionStrategyMap = new HashMap<String, RetentionStrategy>();

    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
          ZKMetadataProvider.getOfflineResourceZKMetadata(_pinotHelixResourceManager.getPropertyStore(), resourceName);

      if (offlineDataResourceZKMetadata.getPushFrequency().equalsIgnoreCase(REFRESH)) {
        LOGGER.info("Resource: " + resourceName + " is a fresh only data resource.");
        return tableToDeletionStrategyMap;
      } else {
        try {
          TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(offlineDataResourceZKMetadata.getRetentionTimeUnit(),
              offlineDataResourceZKMetadata.getRetentionTimeValue());
          tableToDeletionStrategyMap.put(resourceName + ".*", timeRetentionStrategy);
        } catch (Exception e) {
          LOGGER.error("Error creating TimeRetentionStrategy for resource: " + resourceName + ", Exception: " + e);
        }

        for (String key : offlineDataResourceZKMetadata.getMetadata().keySet()) {
          if (key.endsWith("." + CommonConstants.Helix.DataSource.PUSH_FREQUENCY)) {
            String tableName = key.split(".", 2)[0];
            if (!offlineDataResourceZKMetadata.getMetadata().get(key).equalsIgnoreCase(REFRESH)) {
              LOGGER.info("Resource.table - " + resourceName + "." + tableName + " is a fresh only data table.");
            } else {
              if (offlineDataResourceZKMetadata.getMetadata().containsKey(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT) &&
                  offlineDataResourceZKMetadata.getMetadata().containsKey(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE)) {
                try {
                  TimeRetentionStrategy timeRetentionStrategy =
                      new TimeRetentionStrategy(offlineDataResourceZKMetadata.getMetadata().get(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT),
                          offlineDataResourceZKMetadata.getMetadata().get(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE));
                  tableToDeletionStrategyMap.put(resourceName + "." + tableName, timeRetentionStrategy);
                } catch (Exception e) {
                  LOGGER.error("Error creating TimeRetentionStrategy for resource.table: " + resourceName + "." + tableName + ", Exception: " + e);
                }
              }
            }
          }
        }
      }
    }
    return tableToDeletionStrategyMap;
  }

  private void updateSegmentMetadataForEntireCluster() {
    List<String> resourceNames = _pinotHelixResourceManager.getAllPinotResourceNames();
    for (String resourceName : resourceNames) {
      _segmentMetadataMap.put(resourceName, retrieveSegmentMetadataForResource(resourceName));
    }
  }

  private List<SegmentZKMetadata> retrieveSegmentMetadataForResource(String resourceName) {
    List<SegmentZKMetadata> segmentMetadataList = new ArrayList<SegmentZKMetadata>();
    switch (BrokerRequestUtils.getResourceTypeFromResourceName(resourceName)) {
      case OFFLINE:
        List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas = ZKMetadataProvider.getOfflineResourceZKMetadataListForResource(_propertyStore, resourceName);
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
          segmentMetadataList.add(offlineSegmentZKMetadata);
        }
        break;
      case REALTIME:
        List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadatas = ZKMetadataProvider.getRealtimeResourceZKMetadataListForResource(_propertyStore, resourceName);
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
