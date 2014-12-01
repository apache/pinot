package com.linkedin.pinot.server.starter.helix;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
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

  public SegmentOnlineOfflineStateModelFactory(DataManager instanceDataManager,
      SegmentMetadataLoader segmentMetadataLoader) {
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
    final SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new SegmentOnlineOfflineStateModel();
    return SegmentOnlineOfflineStateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class SegmentOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOnlineOfflineStateModel.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {

      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();
      final String pathToPropertyStore = "/" + StringUtil.join("/", resourceName, segmentId);

      final ZNRecord record =
          context.getManager().getHelixPropertyStore().get(pathToPropertyStore, null, AccessOption.PERSISTENT);
      LOGGER.info("Trying to load segment : " + segmentId + " for resource : " + resourceName);
      try {
        SegmentMetadata segmentMetadataForCheck = new SegmentMetadataImpl(record);
        SegmentMetadata segmentMetadataFromServer =
            INSTANCE_DATA_MANAGER.getSegmentMetadata(resourceName, segmentMetadataForCheck.getName());
        if (!isNewSegmentMetadata(segmentMetadataFromServer, segmentMetadataForCheck)) {
          LOGGER.info("Segment is already existed, will do nothing.");
        }
        final String uri = record.getSimpleField(V1Constants.SEGMENT_DOWNLOAD_URL);
        final String localSegmentDir = downloadSegmentToLocal(uri, resourceName, segmentId);
        final SegmentMetadata segmentMetadata =
            SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
        INSTANCE_DATA_MANAGER.addSegment(segmentMetadata);
      } catch (final Exception e) {
        e.printStackTrace();
        LOGGER.error("Cannot load segment : " + segmentId + "!\n", e);
      }
    }

    private boolean isNewSegmentMetadata(SegmentMetadata segmentMetadataFromServer,
        SegmentMetadata segmentMetadataForCheck) {
      if (segmentMetadataFromServer == null) {
        return true;
      }
      if (segmentMetadataForCheck.getVersion() != segmentMetadataFromServer.getVersion()) {
        return false;
      }
      if (segmentMetadataFromServer.getCrc() != segmentMetadataForCheck.getCrc()) {
        return false;
      }
      return true;
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      final String segmentId = message.getPartitionName();
      try {
        INSTANCE_DATA_MANAGER.removeSegment(segmentId);
      } catch (final Exception e) {
        LOGGER.error("Cannot unload the segment : " + segmentId + "!\n" + e.getMessage());
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();
      try {
        final String segmentDir = getSegmentLocalDirectory(resourceName, segmentId);
        FileUtils.deleteDirectory(new File(segmentDir));
      } catch (final Exception e) {
        LOGGER.error("Cannot delete the segment : " + segmentId + " from local directory!\n" + e.getMessage());
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        LOGGER.error(e.getMessage());
      }
    }

    private static String downloadSegmentToLocal(String uri, String resourceName, String segmentId) throws Exception {

      List<File> uncompressedFiles = null;
      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        if (uri.startsWith("http:")) {
          System.out.println(INSTANCE_DATA_MANAGER.getSegmentFileDirectory());
          final File tempFile = new File(INSTANCE_DATA_MANAGER.getSegmentFileDirectory(), segmentId + ".tar.gz");
          final long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
          LOGGER.info("Http GET response content length: " + httpGetResponseContentLength
              + ", Length of downloaded file : " + tempFile.length());
          LOGGER.info("Downloaded file from " + uri);
          uncompressedFiles =
              TarGzCompressionUtils.unTar(tempFile, new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(),
                  resourceName));
          FileUtils.deleteQuietly(tempFile);
        } else {
          uncompressedFiles =
              TarGzCompressionUtils.unTar(new File(uri), new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(),
                  resourceName));
        }
        final File segmentDir =
            new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), resourceName), segmentId);
        LOGGER.info("Uncompressed segment into " + segmentDir.getAbsolutePath());
        Thread.sleep(100);
        if ((uncompressedFiles.size() > 0) && !segmentId.equals(uncompressedFiles.get(0).getName())) {
          if (segmentDir.exists()) {
            LOGGER.info("Deleting the directory and recreating it again- " + segmentDir.getAbsolutePath());
            FileUtils.deleteDirectory(segmentDir);
          }
          final File srcDir = uncompressedFiles.get(0);
          LOGGER.warn("The directory - " + segmentDir.getAbsolutePath()
              + " doesn't exist. Would try to rename the dir - " + srcDir.getAbsolutePath()
              + " to it. The segment id is - " + segmentId);
          FileUtils.moveDirectory(srcDir, segmentDir);
          if (!new File(new File(INSTANCE_DATA_MANAGER.getSegmentDataDirectory(), resourceName), segmentId).exists()) {
            throw new IllegalStateException("The index directory hasn't been created");
          } else {
            LOGGER.info("Was able to succesfully rename the dir to match the segmentId - " + segmentId);
          }
        }
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
