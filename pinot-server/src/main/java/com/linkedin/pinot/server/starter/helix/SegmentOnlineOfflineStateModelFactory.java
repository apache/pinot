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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now servring segment.
 * 3. Delete an existed segment.
 *
 * @author xiafu
 *
 */
public class SegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private static DataManager INSTANCE_DATA_MANAGER;
  private static SegmentMetadataLoader SEGMENT_METADATA_LOADER;
  private static String INSTANCE_ID;
  private static String HELIX_CLUSTER_NAME;

  public SegmentOnlineOfflineStateModelFactory(String helixClusterName, String instanceId, DataManager instanceDataManager,
      SegmentMetadataLoader segmentMetadataLoader) {
    HELIX_CLUSTER_NAME = helixClusterName;
    INSTANCE_ID = instanceId;
    INSTANCE_DATA_MANAGER = instanceDataManager;
    SEGMENT_METADATA_LOADER = instanceDataManager.getSegmentMetadataLoader();
  }

  public SegmentOnlineOfflineStateModelFactory() {
  }

  public static String getStateModelDef() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new SegmentOnlineOfflineStateModel(HELIX_CLUSTER_NAME, INSTANCE_ID);
    return SegmentOnlineOfflineStateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class SegmentOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOnlineOfflineStateModel.class);
    private final String _helixClusterName;
    private final String _instanceId;

    public SegmentOnlineOfflineStateModel(String helixClusterName, String instanceId) {
      _helixClusterName = helixClusterName;
      _instanceId = instanceId;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      final ResourceType resourceType = BrokerRequestUtils.getResourceTypeFromResourceName(message.getResourceName());
      try {
        switch (resourceType) {
          case OFFLINE:
            onBecomeOnlineFromOfflineForOfflineSegment(message, context);
            break;
          case REALTIME:
            onBecomeOnlineFromOfflineForRealtimeSegment(message, context);
            break;

          default:
            throw new RuntimeException("Not supported resource Type for onBecomeOnlineFromOffline message: " + message);
        }
      } catch (Exception e) {
        throw new RuntimeException("Got internal error for adding segment: " + message + "\nException: " + e.getMessage());
      }
    }

    private void onBecomeOnlineFromOfflineForRealtimeSegment(Message message, NotificationContext context) throws Exception {
      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();

      ZkBaseDataAccessor<ZNRecord> baseAccessor =
          (ZkBaseDataAccessor<ZNRecord>) context.getManager().getHelixDataAccessor().getBaseDataAccessor();
      String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _helixClusterName);
      ZkHelixPropertyStore<ZNRecord> propertyStore = new ZkHelixPropertyStore<ZNRecord>(baseAccessor, propertyStorePath, Arrays.asList(propertyStorePath));
      SegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, resourceName, segmentId);
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(propertyStore, _instanceId);
      RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = ZKMetadataProvider.getRealtimeResourceZKMetadata(propertyStore, resourceName);

      ((InstanceDataManager) INSTANCE_DATA_MANAGER).addSegment(propertyStore, realtimeDataResourceZKMetadata, instanceZKMetadata, realtimeSegmentZKMetadata);
    }

    private void onBecomeOnlineFromOfflineForOfflineSegment(Message message, NotificationContext context) {
      // TODO: Need to revisit this part to see if it's possible to add offline segment by just giving
      // OfflineSegmentZKMetadata to InstanceDataManager.

      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();
      final String pathToPropertyStore = "/" + StringUtil.join("/", resourceName, segmentId);

      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(context.getManager().getHelixPropertyStore(), resourceName, segmentId);

      LOGGER.info("Trying to load segment : " + segmentId + " for resource : " + resourceName);
      try {
        SegmentMetadata segmentMetadataForCheck = new SegmentMetadataImpl(offlineSegmentZKMetadata);
        SegmentMetadata segmentMetadataFromServer =
            INSTANCE_DATA_MANAGER.getSegmentMetadata(resourceName, segmentMetadataForCheck.getName());
        if (segmentMetadataFromServer == null) {
          final String localSegmentDir =
              new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), resourceName), segmentId).toString();
          if (new File(localSegmentDir).exists()) {
            try {
              segmentMetadataFromServer = SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
            } catch (Exception e) {
              LOGGER.error("Failed to load segment metadata from local: " + localSegmentDir);
              FileUtils.deleteQuietly(new File(localSegmentDir));
              segmentMetadataFromServer = null;
            }
            try {
              if (!isNewSegmentMetadata(segmentMetadataFromServer, segmentMetadataForCheck)) {
                LOGGER.info("Trying to bootstrap segment from local!");
                INSTANCE_DATA_MANAGER.addSegment(segmentMetadataFromServer);
                return;
              }
            } catch (Exception e) {
              LOGGER.error("Failed to load segment from local, will try to reload it from controller!");
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
          final String uri = offlineSegmentZKMetadata.getDownloadUrl();
          final String localSegmentDir = downloadSegmentToLocal(uri, resourceName, segmentId);
          final SegmentMetadata segmentMetadata =
              SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
          INSTANCE_DATA_MANAGER.addSegment(segmentMetadata);
        } else {
          LOGGER.info("Get already loaded segment again, will do nothing.");
        }

      } catch (final Exception e) {
        LOGGER.error("Cannot load segment : " + segmentId + "!\n", e);
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
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();
      try {
        final String segmentDir = getSegmentLocalDirectory(resourceName, segmentId);
        FileUtils.deleteDirectory(new File(segmentDir));
      } catch (final Exception e) {
        LOGGER.error("Cannot delete the segment : " + segmentId + " from local directory!\n" + e.getMessage(), e);
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.debug("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }

    private static String downloadSegmentToLocal(String uri, String resourceName, String segmentId) throws Exception {

      List<File> uncompressedFiles = null;
      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        File tempSegmentFile =
            new File(INSTANCE_DATA_MANAGER.getSegmentFileDirectory() + "/" + resourceName + "/temp_" + segmentId + "_"
                + System.currentTimeMillis());
        if (uri.startsWith("http:")) {
          final File tempFile = new File(INSTANCE_DATA_MANAGER.getSegmentFileDirectory(), segmentId + ".tar.gz");
          final long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
          LOGGER.info("Downloaded file from " + uri + " to " + tempFile + "; Http GET response content length: "
              + httpGetResponseContentLength + ", Length of downloaded file : " + tempFile.length());
          LOGGER.info("Trying to uncompress segment tar file from " + tempFile + " to " + tempSegmentFile);
          uncompressedFiles = TarGzCompressionUtils.unTar(tempFile, tempSegmentFile);
          FileUtils.deleteQuietly(tempFile);
        } else {
          uncompressedFiles = TarGzCompressionUtils.unTar(new File(uri), tempSegmentFile);
        }
        final File segmentDir =
            new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), resourceName), segmentId);
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
      }
    }

    private String getSegmentLocalDirectory(String resourceName, String segmentId) {
      final String segmentDir = INSTANCE_DATA_MANAGER.getSegmentDataDirectory() + "/" + resourceName + "/" + segmentId;
      return segmentDir;
    }

  }

}
