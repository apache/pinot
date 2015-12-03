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
package com.linkedin.pinot.server.starter.helix;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now serving segment.
 * 3. Delete an existed segment.
 *
 *
 */
public class SegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private DataManager INSTANCE_DATA_MANAGER;
  private SegmentMetadataLoader SEGMENT_METADATA_LOADER;
  private final String INSTANCE_ID;
  private static String HELIX_CLUSTER_NAME;
  private static int SEGMENT_LOAD_MAX_RETRY_COUNT;
  private static long SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;

  public SegmentOnlineOfflineStateModelFactory(String helixClusterName, String instanceId,
      DataManager instanceDataManager, SegmentMetadataLoader segmentMetadataLoader, Configuration pinotHelixProperties,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this.propertyStore = propertyStore;
    HELIX_CLUSTER_NAME = helixClusterName;
    INSTANCE_ID = instanceId;
    INSTANCE_DATA_MANAGER = instanceDataManager;
    SEGMENT_METADATA_LOADER = instanceDataManager.getSegmentMetadataLoader();

    int maxRetries = Integer.parseInt(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MAX_RETRY_COUNT);
    try {
      maxRetries =
          pinotHelixProperties.getInt(CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MAX_RETRY_COUNT, maxRetries);
    } catch (Exception e) {
      // Keep the default value
    }
    SEGMENT_LOAD_MAX_RETRY_COUNT = maxRetries;

    long minRetryDelayMillis = Long.parseLong(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS);
    try {
      minRetryDelayMillis =
          pinotHelixProperties.getLong(CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS,
              minRetryDelayMillis);
    } catch (Exception e) {
      // Keep the default value
    }
    SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS = minRetryDelayMillis;
  }

  public static String getStateModelDef() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel =
        new SegmentOnlineOfflineStateModel(HELIX_CLUSTER_NAME, INSTANCE_ID);
    return SegmentOnlineOfflineStateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class SegmentOnlineOfflineStateModel extends StateModel {
    private final Logger LOGGER = LoggerFactory.getLogger(INSTANCE_ID + " - " + SegmentOnlineOfflineStateModel.class);
    private final String _helixClusterName;
    private final String _instanceId;

    public SegmentOnlineOfflineStateModel(String helixClusterName, String instanceId) {
      _helixClusterName = helixClusterName;
      _instanceId = instanceId;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      final TableType tableType = TableNameBuilder.getTableTypeFromTableName(message.getResourceName());
      try {
        switch (tableType) {
          case OFFLINE:
            onBecomeOnlineFromOfflineForOfflineSegment(message, context);
            break;
          case REALTIME:
            onBecomeOnlineFromOfflineForRealtimeSegment(message, context);
            break;

          default:
            throw new RuntimeException("Not supported table Type for onBecomeOnlineFromOffline message: " + message);
        }
      } catch (Exception e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error(
              "Caught exception in state transition for OFFLINE -> ONLINE for partition" + message.getPartitionName()
                  + " of table " + message.getResourceName(), e);
        }
        Utils.rethrowException(e);
      }
    }

    private void onBecomeOnlineFromOfflineForRealtimeSegment(Message message, NotificationContext context)
        throws Exception {
      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();

      SegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableName, segmentId);
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(propertyStore, _instanceId);
      AbstractTableConfig tableConfig = ZKMetadataProvider.getRealtimeTableConfig(propertyStore, tableName);
      ((InstanceDataManager) INSTANCE_DATA_MANAGER).addSegment(propertyStore, tableConfig, instanceZKMetadata,
          realtimeSegmentZKMetadata);
    }

    private void onBecomeOnlineFromOfflineForOfflineSegment(Message message, NotificationContext context) {
      // TODO: Need to revisit this part to see if it's possible to add offline segment by just giving
      // OfflineSegmentZKMetadata to InstanceDataManager.

      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();

      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, segmentId);

      LOGGER.info("Trying to load segment : " + segmentId + " for table : " + tableName);
      try {
        SegmentMetadata segmentMetadataForCheck = new SegmentMetadataImpl(offlineSegmentZKMetadata);
        SegmentMetadata segmentMetadataFromServer =
            INSTANCE_DATA_MANAGER.getSegmentMetadata(tableName, segmentMetadataForCheck.getName());
        if (segmentMetadataFromServer == null) {
          final String localSegmentDir =
              new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), tableName), segmentId).toString();
          if (new File(localSegmentDir).exists()) {
            try {
              segmentMetadataFromServer = SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
            } catch (Exception e) {
              LOGGER.error("Failed to load segment metadata from local: " + localSegmentDir,e);
              FileUtils.deleteQuietly(new File(localSegmentDir));
              segmentMetadataFromServer = null;
            }
            try {
              if (!isNewSegmentMetadata(segmentMetadataFromServer, segmentMetadataForCheck)) {
                LOGGER.info("Trying to bootstrap segment from local!");
                AbstractTableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(propertyStore, tableName);
                INSTANCE_DATA_MANAGER.addSegment(segmentMetadataFromServer, tableConfig);
                return;
              }
            } catch (Exception e) {
              LOGGER.error("Failed to load segment from local, will try to reload it from controller!",e);
              FileUtils.deleteQuietly(new File(localSegmentDir));
              segmentMetadataFromServer = null;
            }
          }
        }
        if (isNewSegmentMetadata(segmentMetadataFromServer, segmentMetadataForCheck)) {
          if (segmentMetadataFromServer == null) {
            LOGGER.info("Loading new segment from controller - " + segmentMetadataForCheck.getName());
          } else {
            LOGGER.info("Trying to refresh a segment with new data.");
          }
          int retryCount;
          for (retryCount = 0; retryCount < SEGMENT_LOAD_MAX_RETRY_COUNT; ++retryCount) {
            long attemptStartTime = System.currentTimeMillis();
            try {
              AbstractTableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(propertyStore, tableName);
              final String uri = offlineSegmentZKMetadata.getDownloadUrl();
              final String localSegmentDir = downloadSegmentToLocal(uri, tableName, segmentId);
              final SegmentMetadata segmentMetadata =
                  SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
              INSTANCE_DATA_MANAGER.addSegment(segmentMetadata, tableConfig);

              // Successfully loaded the segment, break out of the retry loop
              break;
            } catch (Exception e) {
              long attemptDurationMillis = System.currentTimeMillis() - attemptStartTime;
              LOGGER.warn("Caught exception while loading segment " + segmentId + ", attempt " + (retryCount + 1)
                  + " of " + SEGMENT_LOAD_MAX_RETRY_COUNT, e);

              // Do we need to wait for the next retry attempt?
              if (retryCount < SEGMENT_LOAD_MAX_RETRY_COUNT) {
                // Exponentially back off, wait for (minDuration + attemptDurationMillis) * 1.0..(2^retryCount)+1.0
                double maxRetryDurationMultiplier = Math.pow(2.0, (retryCount + 1));
                double retryDurationMultiplier = Math.random() * maxRetryDurationMultiplier + 1.0;
                long waitTime =
                    (long) ((SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS + attemptDurationMillis) * retryDurationMultiplier);

                LOGGER.warn("Waiting for " + TimeUnit.MILLISECONDS.toSeconds(waitTime) + " seconds to retry");
                long waitEndTime = System.currentTimeMillis() + waitTime;
                while (System.currentTimeMillis() < waitEndTime) {
                  try {
                    Thread.sleep(Math.max(System.currentTimeMillis() - waitEndTime, 1L));
                  } catch (InterruptedException ie) {
                    // Ignore spurious wakeup
                  }
                }
              }
            }
          }
          if (SEGMENT_LOAD_MAX_RETRY_COUNT <= retryCount) {
            String msg = "Failed to load segment " + segmentId + " after " + retryCount + " retries";
            LOGGER.error(msg);
            throw new RuntimeException(msg);
          }
        } else {
          LOGGER.info("Get already loaded segment again, will do nothing.");
        }

      } catch (final Exception e) {
        LOGGER.error("Cannot load segment : " + segmentId + "!\n", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    private boolean isNewSegmentMetadata(SegmentMetadata segmentMetadataFromServer,
        SegmentMetadata segmentMetadataForCheck) {
      if (segmentMetadataFromServer == null || segmentMetadataForCheck == null) {
        return true;
      }
      if ((!segmentMetadataFromServer.getCrc().equalsIgnoreCase("null"))
          && (segmentMetadataFromServer.getCrc().equals(segmentMetadataForCheck.getCrc()))) {
        return false;
      }
      return true;
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      final String segmentId = message.getPartitionName();
      try {
        INSTANCE_DATA_MANAGER.removeSegment(segmentId);
      } catch (final Exception e) {
        LOGGER.error("Cannot unload the segment : " + segmentId + "!\n" + e.getMessage(), e);
        Utils.rethrowException(e);
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();
      try {
        final File segmentDir = new File(getSegmentLocalDirectory(tableName, segmentId));
        if (segmentDir.exists()) {
          FileUtils.deleteQuietly(segmentDir);
        }
      } catch (final Exception e) {
        LOGGER.error("Cannot delete the segment : " + segmentId + " from local directory!\n" + e.getMessage(), e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        LOGGER.error("Caught exception on ONLINE -> DROPPED state transition", e);
        Utils.rethrowException(e);
      }
    }

    private String downloadSegmentToLocal(String uri, String tableName, String segmentId) throws Exception {
      File tempSegmentFile = null;
      File tempFile = null;
      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        try {
          tempSegmentFile =
              new File(INSTANCE_DATA_MANAGER.getSegmentFileDirectory() + "/" + tableName + "/temp_" + segmentId
                  + "_" + System.currentTimeMillis());
          if (uri.startsWith("http:") || uri.startsWith("https:")) {
            tempFile = new File(INSTANCE_DATA_MANAGER.getSegmentFileDirectory(), segmentId + ".tar.gz");
            final long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
            LOGGER.info("Downloaded file from " + uri + " to " + tempFile + "; Http GET response content length: "
                + httpGetResponseContentLength + ", Length of downloaded file : " + tempFile.length());
            LOGGER.info("Trying to uncompress segment tar file from " + tempFile + " to " + tempSegmentFile);
            TarGzCompressionUtils.unTar(tempFile, tempSegmentFile);
            FileUtils.deleteQuietly(tempFile);
          } else {
            TarGzCompressionUtils.unTar(new File(uri), tempSegmentFile);
          }
          final File segmentDir =
              new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), tableName), segmentId);
          Thread.sleep(1000);
          if (segmentDir.exists()) {
            LOGGER.info("Deleting the directory and recreating it again- " + segmentDir.getAbsolutePath());
            FileUtils.deleteDirectory(segmentDir);
          }
          LOGGER.info("Move the dir - " + tempSegmentFile.listFiles()[0] + " to " + segmentDir.getAbsolutePath()
              + ". The segment id is - " + segmentId);
          FileUtils.moveDirectory(tempSegmentFile.listFiles()[0], segmentDir);
          FileUtils.deleteDirectory(tempSegmentFile);
          Thread.sleep(1000);
          LOGGER.info("Was able to succesfully rename the dir to match the segmentId - " + segmentId);

          new File(segmentDir, "finishedLoading").createNewFile();
          return segmentDir.getAbsolutePath();
        } catch (Exception e) {
          FileUtils.deleteQuietly(tempSegmentFile);
          FileUtils.deleteQuietly(tempFile);
          LOGGER.error("Caught exception", e);
          Utils.rethrowException(e);
          throw new AssertionError("Should not reach this");
        }
      }
    }

    private String getSegmentLocalDirectory(String tableName, String segmentId) {
      final String segmentDir = INSTANCE_DATA_MANAGER.getSegmentDataDirectory() + "/" + tableName + "/" + segmentId;
      return segmentDir;
    }

  }

}
