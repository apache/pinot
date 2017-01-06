/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SegmentName;


/**
 *
 */
public class SegmentDeletionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 3600L;
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  private final ScheduledExecutorService _executorService;
  private final String _localDiskDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String DELETED_SEGMENTS = "Deleted_Segments";

  SegmentDeletionManager(String localDiskDir, HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _localDiskDir = localDiskDir;
    _helixAdmin = helixAdmin;
    _helixClusterName = helixClusterName;
    _propertyStore = propertyStore;

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotHelixResourceManagerExecutorService");
        return thread;
      }
    });
  }

  public void stop() {
    _executorService.shutdownNow();
  }

  public void deleteSegment(final String tableName, final String segmentId) {
    deleteSegmentWithDelay(tableName, segmentId, DEFAULT_DELETION_DELAY_SECONDS);
  }

  private void deleteSegmentWithDelay(final String tableName, final String segmentId, final long deletionDelaySeconds) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        deleteSegmentFromPropertyStoreAndLocal(tableName, segmentId, deletionDelaySeconds);
      }
    }, deletionDelaySeconds, TimeUnit.SECONDS);
  }

  /**
   * Check if segment got deleted from IdealStates and ExternalView.
   * If segment got removed, then delete this segment from PropertyStore and local disk.
   *
   * @param tableName
   * @param segmentId
   */
  private synchronized void deleteSegmentFromPropertyStoreAndLocal(String tableName, String segmentId, long deletionDelay) {
    // Check if segment got removed from ExternalView and IdealStates
    if (_helixAdmin.getResourceExternalView(_helixClusterName, tableName) == null ||
        _helixAdmin.getResourceIdealState(_helixClusterName, tableName) == null) {
      LOGGER.warn("Resource: {} is not set up in idealState or ExternalView, won't do anything", tableName);
      return;
    }

    boolean isSegmentReadyToDelete = false;
    try {
      Map<String, String> segmentToInstancesMapFromExternalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName).getStateMap(segmentId);
      Map<String, String> segmentToInstancesMapFromIdealStates = _helixAdmin.getResourceIdealState(_helixClusterName, tableName).getInstanceStateMap(segmentId);
      if ((segmentToInstancesMapFromExternalView == null || segmentToInstancesMapFromExternalView.isEmpty())
          && (segmentToInstancesMapFromIdealStates == null || segmentToInstancesMapFromIdealStates.isEmpty())) {
        isSegmentReadyToDelete = true;
      } else {
        long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
        LOGGER.info("Segment: {} is still in IdealStates: {} or ExternalView: {}, will retry in {} seconds.", segmentId,
            segmentToInstancesMapFromIdealStates, segmentToInstancesMapFromExternalView, effectiveDeletionDelay);
        deleteSegmentWithDelay(tableName, segmentId, effectiveDeletionDelay);
        return;
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while processing segment " + segmentId, e);
      isSegmentReadyToDelete = true;
    }

    if (isSegmentReadyToDelete) {
      String segmentPropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentId);
      LOGGER.info("Trying to delete segment : {} from Property store.", segmentId);
      boolean deletionFromPropertyStoreSuccessful = true;
      if (_propertyStore.exists(segmentPropertyStorePath, AccessOption.PERSISTENT)) {
        deletionFromPropertyStoreSuccessful = _propertyStore.remove(segmentPropertyStorePath, AccessOption.PERSISTENT);
      }

      if (!deletionFromPropertyStoreSuccessful) {
        long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
        LOGGER.warn("Failed to delete segment {} from property store, will retry in {} seconds.", segmentId,
            effectiveDeletionDelay);
        deleteSegmentWithDelay(tableName, segmentId, effectiveDeletionDelay);
        return;
      }

      final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      if (_localDiskDir != null) {
        File fileToMove = new File(new File(_localDiskDir, rawTableName), segmentId);
        if (fileToMove.exists()) {
          File targetDir = new File(new File(_localDiskDir, DELETED_SEGMENTS), rawTableName);
          try {
            // Overwrites the file if it already exists in the target directory.
            FileUtils.copyFileToDirectory(fileToMove, targetDir, true);
            LOGGER.info("Moved segment {} from {} to {}", segmentId, fileToMove.getAbsolutePath(),
                targetDir.getAbsolutePath());
            if (!fileToMove.delete()) {
              LOGGER.warn("Could not delete file", segmentId, fileToMove.getAbsolutePath());
            }
          } catch (IOException e) {
            LOGGER.warn("Could not move segment {} from {} to {}", segmentId, fileToMove.getAbsolutePath(),
                targetDir.getAbsolutePath(), e);
          }
        } else {
          CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
          switch (tableType) {
            case OFFLINE:
              LOGGER.warn("Not found local segment file for segment {}" + fileToMove.getAbsolutePath());
              break;
            case REALTIME:
              if (SegmentName.isLowLevelConsumerSegmentName(segmentId)) {
                LOGGER.warn("Not found local segment file for segment {}" + fileToMove.getAbsolutePath());
              }
              break;
            default:
              LOGGER.warn("Unsupported table type {} when deleting segment {}", tableType, segmentId);
          }
        }
      } else {
        LOGGER.info("localDiskDir is not configured, won't delete segment {} from disk", segmentId);
      }
    } else {
      long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
      LOGGER.info("Segment: {} is still in IdealStates or ExternalView, will retry in {} seconds.", segmentId, effectiveDeletionDelay);
      deleteSegmentWithDelay(tableName, segmentId, effectiveDeletionDelay);
    }
  }
}
