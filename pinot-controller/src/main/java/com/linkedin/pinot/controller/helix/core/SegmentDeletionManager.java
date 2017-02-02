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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SegmentName;

public class SegmentDeletionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 300L;  // Maximum of 5 minutes back-off to retry the deletion
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  private final ScheduledExecutorService _executorService;
  private final String _localDiskDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String DELETED_SEGMENTS = "Deleted_Segments";

  public SegmentDeletionManager(String localDiskDir, HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> propertyStore) {
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

  public void deleteSegments(final String tableName, final Collection<String> segmentIds) {
    deleteSegmentsWithDelay(tableName, segmentIds, DEFAULT_DELETION_DELAY_SECONDS);
  }

  protected void deleteSegmentsWithDelay(final String tableName, final Collection<String> segmentIds,
      final long deletionDelaySeconds) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        deleteSegmentFromPropertyStoreAndLocal(tableName, segmentIds, deletionDelaySeconds);
      }
    }, deletionDelaySeconds, TimeUnit.SECONDS);
  }

  protected synchronized void deleteSegmentFromPropertyStoreAndLocal(String tableName, Collection<String> segmentIds, long deletionDelay) {
    // Check if segment got removed from ExternalView and IdealStates
    if (_helixAdmin.getResourceExternalView(_helixClusterName, tableName) == null
        || _helixAdmin.getResourceIdealState(_helixClusterName, tableName) == null) {
      LOGGER.warn("Resource: {} is not set up in idealState or ExternalView, won't do anything", tableName);
      return;
    }

    List<String> segmentsToDelete = new ArrayList<>(segmentIds.size());
    Set<String> segmentsToRetryLater = new HashSet<>(segmentIds.size());

    try {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);

      for (String segmentId : segmentIds) {
        Map<String, String> segmentToInstancesMapFromExternalView = externalView.getStateMap(segmentId);
        Map<String, String> segmentToInstancesMapFromIdealStates = idealState.getInstanceStateMap(segmentId);
        if ((segmentToInstancesMapFromExternalView == null || segmentToInstancesMapFromExternalView.isEmpty()) && (
            segmentToInstancesMapFromIdealStates == null || segmentToInstancesMapFromIdealStates.isEmpty())) {
          segmentsToDelete.add(segmentId);
        } else {
          segmentsToRetryLater.add(segmentId);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while checking helix states for table {} " + tableName, e);
      segmentsToDelete.clear();
      segmentsToDelete.addAll(segmentIds);
      segmentsToRetryLater.clear();
    }

    int nSegmentsDeleted = 0;
    if (!segmentsToDelete.isEmpty()) {
      List<String> propStorePathList = new ArrayList<>(segmentsToDelete.size());
      for (String segmentId : segmentsToDelete) {
        String segmentPropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentId);
        propStorePathList.add(segmentPropertyStorePath);
      }

      boolean[] deleteSuccessful = _propertyStore.remove(propStorePathList, AccessOption.PERSISTENT);
      for (int i = 0; i < deleteSuccessful.length; i++) {
        if (!deleteSuccessful[i]) {
          // remove API can fail because the prop store entry did not exist, so check first.
          if (_propertyStore.exists(propStorePathList.get(i), AccessOption.PERSISTENT)) {
            LOGGER.info("Could not delete {} from propertystore", propStorePathList.get(i));
            segmentsToRetryLater.add(segmentsToDelete.get(i));
          }
        }
      }

      for (String segmentId : segmentsToDelete) {
        if (!segmentsToRetryLater.contains(segmentId)) {
          // This segment does NOT appear in retry.
          removeSegmentFromStore(tableName, segmentId);
          nSegmentsDeleted++;
        }
      }
    }

    LOGGER.info("Deleted {} segments from table {}", nSegmentsDeleted, tableName);

    if (segmentsToRetryLater.size() > 0) {
      long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
      LOGGER.info("Postponing deletion of {} segments from table {}", nSegmentsDeleted, tableName);
      deleteSegmentsWithDelay(tableName, segmentsToRetryLater, effectiveDeletionDelay);
      return;
    }
  }

  protected void removeSegmentFromStore(String tableName, String segmentId) {
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (_localDiskDir != null) {
      File fileToMove = new File(new File(_localDiskDir, rawTableName), segmentId);
      if (fileToMove.exists()) {
        File targetDir = new File(new File(_localDiskDir, DELETED_SEGMENTS), rawTableName);
        try {
          // Overwrites the file if it already exists in the target directory.
          FileUtils.copyFileToDirectory(fileToMove, targetDir, true);
          LOGGER.info("Moved segment {} from {} to {}", segmentId, fileToMove.getAbsolutePath(), targetDir.getAbsolutePath());
          if (!fileToMove.delete()) {
            LOGGER.warn("Could not delete file", segmentId, fileToMove.getAbsolutePath());
          }
        } catch (IOException e) {
          LOGGER.warn("Could not move segment {} from {} to {}", segmentId, fileToMove.getAbsolutePath(), targetDir.getAbsolutePath(), e);
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
  }
}
