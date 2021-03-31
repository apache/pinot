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
package org.apache.pinot.controller.helix.core;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class SegmentDeletionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 300L;  // Maximum of 5 minutes back-off to retry the deletion
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  private final ScheduledExecutorService _executorService;
  private final String _dataDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String DELETED_SEGMENTS = "Deleted_Segments";

  public SegmentDeletionManager(String dataDir, HelixAdmin helixAdmin, String helixClusterName,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _dataDir = dataDir;
    _helixAdmin = helixAdmin;
    _helixClusterName = helixClusterName;
    _propertyStore = propertyStore;
    _executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("PinotHelixResourceManagerExecutorService");
      return thread;
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
    _executorService.schedule(() -> deleteSegmentFromPropertyStoreAndLocal(tableName, segmentIds, deletionDelaySeconds), deletionDelaySeconds, TimeUnit.SECONDS);
  }

  protected synchronized void deleteSegmentFromPropertyStoreAndLocal(String tableName, Collection<String> segmentIds,
      long deletionDelay) {
    // Check if segment got removed from ExternalView or IdealState
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    if (externalView == null || idealState == null) {
      LOGGER.warn("Resource: {} is not set up in idealState or ExternalView, won't do anything", tableName);
      return;
    }

    List<String> segmentsToDelete = new ArrayList<>(segmentIds.size()); // Has the segments that will be deleted
    Set<String> segmentsToRetryLater = new HashSet<>(segmentIds.size());  // List of segments that we need to retry

    try {
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

    if (!segmentsToDelete.isEmpty()) {
      List<String> propStorePathList = new ArrayList<>(segmentsToDelete.size());
      for (String segmentId : segmentsToDelete) {
        String segmentPropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentId);
        propStorePathList.add(segmentPropertyStorePath);
      }

      boolean[] deleteSuccessful = _propertyStore.remove(propStorePathList, AccessOption.PERSISTENT);
      List<String> propStoreFailedSegs = new ArrayList<>(segmentsToDelete.size());
      for (int i = 0; i < deleteSuccessful.length; i++) {
        final String segmentId = segmentsToDelete.get(i);
        if (!deleteSuccessful[i]) {
          // remove API can fail because the prop store entry did not exist, so check first.
          if (_propertyStore.exists(propStorePathList.get(i), AccessOption.PERSISTENT)) {
            LOGGER.info("Could not delete {} from propertystore", propStorePathList.get(i));
            segmentsToRetryLater.add(segmentId);
            propStoreFailedSegs.add(segmentId);
          }
        }
      }
      segmentsToDelete.removeAll(propStoreFailedSegs);

      removeSegmentsFromStore(tableName, segmentsToDelete);
    }

    LOGGER.info("Deleted {} segments from table {}:{}", segmentsToDelete.size(), tableName,
        segmentsToDelete.size() <= 5 ? segmentsToDelete : "");

    if (segmentsToRetryLater.size() > 0) {
      long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
      LOGGER.info("Postponing deletion of {} segments from table {}", segmentsToRetryLater.size(), tableName);
      deleteSegmentsWithDelay(tableName, segmentsToRetryLater, effectiveDeletionDelay);
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends PinotFS> void removeSegmentsFromStore(String tableNameWithType, List<String> segments) {
    URI deletedSegmentBaseUri = URIUtils.getUri(_dataDir, DELETED_SEGMENTS);
    T pinotFS = (T) PinotFSFactory.create(deletedSegmentBaseUri.getScheme());
    PinotFS.ParallelOperationsBuilder<T> operationsBuilder = pinotFS.getParallelOperationsBuilder();

    for (String segment : segments) {
        removeSegmentFromStore(tableNameWithType, segment, operationsBuilder);
    }

    try {
      operationsBuilder.execute();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.warn("Failed to delete segments for table '{}': {}", tableNameWithType, segments);
    }
  }

  protected <T extends PinotFS> void removeSegmentFromStore(String tableNameWithType, String segmentId, PinotFS.ParallelOperationsBuilder<T> operationsBuilder) {
    // Ignore HLC segments as they are not stored in Pinot FS
    if (SegmentName.isHighLevelConsumerSegmentName(segmentId)) {
      return;
    }
    if (_dataDir != null) {
      operationsBuilder.add(removeSegmentFromStoreOperation(tableNameWithType, segmentId));
    } else {
      LOGGER.info("dataDir is not configured, won't delete segment {} from disk", segmentId);
    }
  }

  <T extends PinotFS> PinotFS.FileOperation<T> removeSegmentFromStoreOperation(String tableNameWithType, String segmentId) {
    requireNonNull(_dataDir, "data directory is null");
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    URI fileToMoveURI = URIUtils.getUri(_dataDir, rawTableName, URIUtils.encode(segmentId));
    URI deletedSegmentDestURI = URIUtils.getUri(_dataDir, DELETED_SEGMENTS, rawTableName, URIUtils.encode(segmentId));
    return  pinotFS -> {
      try {
        if (pinotFS.exists(fileToMoveURI)) {
          // Overwrites the file if it already exists in the target directory.
          if (pinotFS.move(fileToMoveURI, deletedSegmentDestURI, true)) {
            // Updates last modified.
            // Touch is needed here so that removeAgedDeletedSegments() works correctly.
            boolean result = pinotFS.touch(deletedSegmentDestURI);
            LOGGER.info("Moved segment {} from {} to {}", segmentId, fileToMoveURI.toString(),
                    deletedSegmentDestURI.toString());
            return result;
          }
          else {
            LOGGER.warn("Failed to move segment {} from {} to {}", segmentId, fileToMoveURI.toString(),
                    deletedSegmentDestURI.toString());
          }
        }
        else {
          LOGGER.warn("Failed to find local segment file for segment {}", fileToMoveURI.toString());
        }
      }
      catch (IOException e) {
        LOGGER.warn("Could not move segment {} from {} to {}", segmentId, fileToMoveURI.toString(),
                deletedSegmentDestURI.toString(), e);
      }
      return false;
    };
  }

  /**
   * Removes aged deleted segments from the deleted directory
   * @param retentionInDays: retention for deleted segments in days
   */
  @SuppressWarnings("unchecked")
  public <T extends PinotFS> void removeAgedDeletedSegments(int retentionInDays) {
    if (_dataDir != null) {
      URI deletedDirURI = URIUtils.getUri(_dataDir, DELETED_SEGMENTS);
      T pinotFS = (T) PinotFSFactory.create(deletedDirURI.getScheme());

      try {
        // Check that the directory for deleted segments exists.
        if (!pinotFS.isDirectory(deletedDirURI)) {
          LOGGER.warn("Deleted segment directory {} does not exist or it is not directory.", deletedDirURI.toString());
          return;
        }

        String[] tableNameDirs = pinotFS.listFiles(deletedDirURI, false);
        if (tableNameDirs == null) {
          LOGGER.warn("Deleted segment directory {} does not exist.", deletedDirURI.toString());
          return;
        }

        PinotFS.ParallelOperationsBuilder<T> directoryOperationsBuilder = pinotFS.getParallelOperationsBuilder();
        for (String tableNameDir : tableNameDirs) {
          URI tableNameURI = URIUtils.getUri(tableNameDir);
          // Get files that are aged
          final String[] targetFiles = pinotFS.listFiles(tableNameURI, false);
          AtomicInteger numFilesDeleted = new AtomicInteger();

          PinotFS.ParallelOperationsBuilder<T> operationsBuilder = pinotFS.getParallelOperationsBuilder();
          for (String targetFile : targetFiles) {
            operationsBuilder.add(fs -> {
              URI targetURI = URIUtils.getUri(targetFile);
              Date dateToDelete = DateTime.now().minusDays(retentionInDays).toDate();
              if (pinotFS.lastModified(targetURI) < dateToDelete.getTime()) {
                boolean result = fs.delete(targetURI, true);
                if (!result) {
                  LOGGER.warn("Cannot remove file {} from deleted directory.", targetURI.toString());
                }
                else {
                  numFilesDeleted.incrementAndGet();
                }
                return result;
              } else {
                return true;
              }
            });
          }

          directoryOperationsBuilder.add(fs -> {
            try {
              operationsBuilder.execute();

              if (numFilesDeleted.get() == targetFiles.length) {
                // Delete directory if it's empty
                if (!fs.delete(tableNameURI, false)) {
                  LOGGER.warn("The directory {} cannot be removed.", tableNameDir);
                }
              }
            } catch (ExecutionException | InterruptedException e) {
              LOGGER.error("Had trouble deleting directories: {}", deletedDirURI.toString(), e);
            }
            return true;
          });
        }
        directoryOperationsBuilder.execute();
      } catch (InterruptedException | ExecutionException | IOException e) {
        LOGGER.error("Had trouble deleting directories: {}", deletedDirURI.toString(), e);
      }
    } else {
      LOGGER.info("dataDir is not configured, won't delete any expired segments from deleted directory.");
    }
  }
}
