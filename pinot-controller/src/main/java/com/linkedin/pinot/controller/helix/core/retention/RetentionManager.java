package com.linkedin.pinot.controller.helix.core.retention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.HelixHelper;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import com.linkedin.pinot.controller.helix.core.utils.PinotHelixUtils;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


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

  private final HelixAdmin _helixAdmin;
  private final String _helixClusterName;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _tableDeletionStrategy = new HashMap<String, RetentionStrategy>();
  private final Map<String, List<SegmentMetadata>> _segmentMetadataMap = new HashMap<String, List<SegmentMetadata>>();
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
    _helixAdmin = pinotHelixResourceManager.getHelixAdmin();
    _helixClusterName = pinotHelixResourceManager.getHelixClusterName();
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
      List<SegmentMetadata> segmentMetadataList = _segmentMetadataMap.get(resourceName);
      for (SegmentMetadata segmentMetadata : segmentMetadataList) {
        RetentionStrategy deletionStrategy;
        if (_tableDeletionStrategy.containsKey(resourceName + "." + segmentMetadata.getTableName())) {
          deletionStrategy = _tableDeletionStrategy.get(resourceName + "." + segmentMetadata.getTableName());
        } else {
          deletionStrategy = _tableDeletionStrategy.get(resourceName + ".*");
        }
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: " + segmentMetadata.getName());
          continue;
        }
        if (deletionStrategy.isPurgeable(segmentMetadata)) {
          LOGGER.info("Trying to delete segment: " + segmentMetadata.getName());
          _pinotHelixResourceManager.deleteSegment(resourceName, segmentMetadata.getName());
        }
      }
    }
  }

  private void updateDeletionStrategiesForEntireCluster() {
    List<String> resourceNames = _helixAdmin.getResourcesInCluster(_helixClusterName);
    for (String resourceName : resourceNames) {
      if (resourceName.equals(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        continue;
      }
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
    Map<String, String> resourceConfigs =
        HelixHelper.getResourceConfigsFor(_helixClusterName, resourceName, _helixAdmin);
    if (resourceConfigs.get(CommonConstants.Helix.DataSource.PUSH_FREQUENCY).equalsIgnoreCase(REFRESH)) {
      LOGGER.info("Resource: " + resourceName + " is a fresh only data resource.");
      return tableToDeletionStrategyMap;
    } else {
      try {
        TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(resourceConfigs.get(CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT),
            resourceConfigs.get(CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE));
        tableToDeletionStrategyMap.put(resourceName + ".*", timeRetentionStrategy);
      } catch (Exception e) {
        LOGGER.error("Error creating TimeRetentionStrategy for resource: " + resourceName + ", Exception: " + e);
      }

      for (String key : resourceConfigs.keySet()) {
        if (key.endsWith("." + CommonConstants.Helix.DataSource.PUSH_FREQUENCY)) {
          String tableName = key.split(".", 2)[0];
          if (!resourceConfigs.get(key).equalsIgnoreCase(REFRESH)) {
            LOGGER.info("Resource.table - " + resourceName + "." + tableName + " is a fresh only data table.");
          } else {
            if (resourceConfigs.containsKey(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT) &&
                resourceConfigs.containsKey(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE)) {
              try {
                TimeRetentionStrategy timeRetentionStrategy =
                    new TimeRetentionStrategy(resourceConfigs.get(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT),
                        resourceConfigs.get(tableName + "." + CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE));
                tableToDeletionStrategyMap.put(resourceName + "." + tableName, timeRetentionStrategy);
              } catch (Exception e) {
                LOGGER.error("Error creating TimeRetentionStrategy for resource.table: " + resourceName + "." + tableName + ", Exception: " + e);
              }
            }
          }
        }
      }
    }
    return tableToDeletionStrategyMap;
  }

  private void updateSegmentMetadataForEntireCluster() {
    List<String> resourceNames = _helixAdmin.getResourcesInCluster(_helixClusterName);
    for (String resourceName : resourceNames) {
      if (resourceName.equals(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        continue;
      }
      _segmentMetadataMap.put(resourceName, retrieveSegmentMetadataForResource(resourceName));
    }
  }

  private List<SegmentMetadata> retrieveSegmentMetadataForResource(String resourceName) {
    List<SegmentMetadata> segmentMetadataList = new ArrayList<SegmentMetadata>();
    List<ZNRecord> segmentMetadataZnRecords =
        _propertyStore.getChildren(PinotHelixUtils.constructPropertyStorePathForResource(resourceName), null, AccessOption.PERSISTENT);
    for (ZNRecord segmentMetaZnRecord : segmentMetadataZnRecords) {
      segmentMetadataList.add(new SegmentMetadataImpl(segmentMetaZnRecord));
    }
    return segmentMetadataList;
  }

  public void stop() {
    _executorService.shutdown();

  }

}
