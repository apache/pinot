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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
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

  public static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _resourceDeletionStrategy = new HashMap<String, RetentionStrategy>();
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
      LOGGER.error("Got error in deletion thread: " + e);
    }
  }

  private void scanSegmentMetadataAndPurge() {
    for (String resourceName : _segmentMetadataMap.keySet()) {
      List<SegmentZKMetadata> segmentZKMetadataList = _segmentMetadataMap.get(resourceName);
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        RetentionStrategy deletionStrategy;
        deletionStrategy = _resourceDeletionStrategy.get(resourceName);
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: " + segmentZKMetadata.getSegmentName());
          continue;
        }
        if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
          if (((RealtimeSegmentZKMetadata) segmentZKMetadata).getStatus() == Status.IN_PROGRESS) {
            continue;
          }
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
      _resourceDeletionStrategy.putAll(retrieveDeletionStrategiesForResource(resourceName));
    }
  }

  /**
   *
   * @param resourceName
   * @return resourceName to RetentionStrategy mapping
   */
  private Map<String, RetentionStrategy> retrieveDeletionStrategiesForResource(String resourceName) {

    switch (BrokerRequestUtils.getResourceTypeFromResourceName(resourceName)) {
      case OFFLINE:
        return handleOfflineDeletionStrategy(resourceName);
      case REALTIME:
        return handleRealtimeDeletionStrategy(resourceName);
      default:
        break;
    }
    throw new UnsupportedOperationException("Not support resource type for resource name - " + resourceName);
  }

  private Map<String, RetentionStrategy> handleOfflineDeletionStrategy(String resourceName) {
    Map<String, RetentionStrategy> resourceToDeletionStrategyMap = new HashMap<String, RetentionStrategy>();

    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
        ZKMetadataProvider.getOfflineResourceZKMetadata(_pinotHelixResourceManager.getPropertyStore(), resourceName);

    if (offlineDataResourceZKMetadata.getPushFrequency().equalsIgnoreCase(REFRESH)) {
      LOGGER.info("Resource: " + resourceName + " is a fresh only data resource.");
      return resourceToDeletionStrategyMap;
    } else {
      try {
        TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(offlineDataResourceZKMetadata.getRetentionTimeUnit(),
            offlineDataResourceZKMetadata.getRetentionTimeValue());
        resourceToDeletionStrategyMap.put(resourceName, timeRetentionStrategy);
      } catch (Exception e) {
        LOGGER.error("Error creating TimeRetentionStrategy for resource: " + resourceName + ", Exception: " + e);
      }
      return resourceToDeletionStrategyMap;
    }
  }

  private Map<String, RetentionStrategy> handleRealtimeDeletionStrategy(String resourceName) {
    Map<String, RetentionStrategy> resourceToDeletionStrategyMap = new HashMap<String, RetentionStrategy>();
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata =
        ZKMetadataProvider.getRealtimeResourceZKMetadata(_pinotHelixResourceManager.getPropertyStore(), resourceName);
    try {
      TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(realtimeDataResourceZKMetadata.getRetentionTimeUnit(),
          realtimeDataResourceZKMetadata.getRetentionTimeValue());
      resourceToDeletionStrategyMap.put(resourceName, timeRetentionStrategy);
    } catch (Exception e) {
      LOGGER.error("Error creating TimeRetentionStrategy for resource: " + resourceName + ", Exception: " + e);
    }
    return resourceToDeletionStrategyMap;
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
        List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas =
            ZKMetadataProvider.getOfflineResourceZKMetadataListForResource(_pinotHelixResourceManager.getPropertyStore(), resourceName);
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
          segmentMetadataList.add(offlineSegmentZKMetadata);
        }
        break;
      case REALTIME:
        List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadatas =
            ZKMetadataProvider.getRealtimeResourceZKMetadataListForResource(_pinotHelixResourceManager.getPropertyStore(), resourceName);
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
