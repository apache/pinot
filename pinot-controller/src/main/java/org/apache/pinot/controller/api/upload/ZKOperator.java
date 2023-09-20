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
package org.apache.pinot.controller.api.upload;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.FileUploadDownloadClient.FileUploadType;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.util.ZKMetadataUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ZKOperator is a util class that is used during segment upload to set relevant metadata fields in zk. It will
 * currently
 * also perform the data move. In the future when we introduce versioning, we will decouple these two steps.
 * TODO: Merge it into PinotHelixResourceManager
 */
public class ZKOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZKOperator.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;

  public ZKOperator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
  }

  public void completeSegmentOperations(String tableNameWithType, SegmentMetadata segmentMetadata,
      FileUploadType uploadType, @Nullable URI finalSegmentLocationURI, File segmentFile,
      @Nullable String sourceDownloadURIStr, String segmentDownloadURIStr, @Nullable String crypterName,
      long segmentSizeInBytes, boolean enableParallelPushProtection, boolean allowRefresh, HttpHeaders headers)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    boolean refreshOnly =
        Boolean.parseBoolean(headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY));

    ZNRecord existingSegmentMetadataZNRecord =
        _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    if (existingSegmentMetadataZNRecord != null && shouldProcessAsNewSegment(tableNameWithType, segmentName,
        existingSegmentMetadataZNRecord, enableParallelPushProtection)) {
      LOGGER.warn("Removing segment ZK metadata (recovering from previous upload failure) for table: {}, segment: {}",
          tableNameWithType, segmentName);
      Preconditions.checkState(_pinotHelixResourceManager.removeSegmentZKMetadata(tableNameWithType, segmentName),
          "Failed to remove segment ZK metadata for table: %s, segment: %s", tableNameWithType, segmentName);
      existingSegmentMetadataZNRecord = null;
    }

    if (existingSegmentMetadataZNRecord == null) {
      // Add a new segment
      if (refreshOnly) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot refresh non-existing segment: %s for table: %s", segmentName, tableNameWithType),
            Response.Status.GONE);
      }
      LOGGER.info("Adding new segment: {} to table: {}", segmentName, tableNameWithType);
      processNewSegment(tableNameWithType, segmentMetadata, uploadType, finalSegmentLocationURI, segmentFile,
          sourceDownloadURIStr, segmentDownloadURIStr, crypterName, segmentSizeInBytes, enableParallelPushProtection,
          headers);
    } else {
      // Refresh an existing segment
      if (!allowRefresh) {
        // We cannot perform this check up-front in UploadSegment API call. If a segment doesn't exist during the check
        // done up-front but ends up getting created before the check here, we could incorrectly refresh an existing
        // segment.
        throw new ControllerApplicationException(LOGGER,
            String.format("Segment: %s already exists in table: %s. Refresh not permitted.", segmentName,
                tableNameWithType), Response.Status.CONFLICT);
      }
      LOGGER.info("Segment: {} already exists in table: {}, refreshing it", segmentName, tableNameWithType);
      processExistingSegment(tableNameWithType, segmentMetadata, uploadType, existingSegmentMetadataZNRecord,
          finalSegmentLocationURI, segmentFile, sourceDownloadURIStr, segmentDownloadURIStr, crypterName,
          segmentSizeInBytes, enableParallelPushProtection, headers);
    }
  }

  /**
   * Returns {@code true} when the segment should be processed as new segment.
   * <p>When segment ZK metadata exists, check if segment exists in the ideal state. If the previous upload failed after
   * segment ZK metadata is created but before assigning the segment to the ideal state, we want to remove the existing
   * segment ZK metadata and treat it as a new segment.
   */
  private boolean shouldProcessAsNewSegment(String tableNameWithType, String segmentName,
      ZNRecord existingSegmentMetadataZNRecord, boolean enableParallelPushProtection) {
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
    if (idealState.getInstanceStateMap(segmentName) != null) {
      return false;
    }
    // Segment does not exist in the ideal state
    if (enableParallelPushProtection) {
      // Check segment upload start time when parallel push protection is enabled in case the segment is being uploaded
      long segmentUploadStartTime = new SegmentZKMetadata(existingSegmentMetadataZNRecord).getSegmentUploadStartTime();
      if (segmentUploadStartTime > 0) {
        handleParallelPush(tableNameWithType, segmentName, segmentUploadStartTime);
      }
    }
    return true;
  }

  private void handleParallelPush(String tableNameWithType, String segmentName, long segmentUploadStartTime) {
    assert segmentUploadStartTime > 0;
    if (System.currentTimeMillis() - segmentUploadStartTime > _controllerConf.getSegmentUploadTimeoutInMillis()) {
      // Last segment upload does not finish properly, replace the segment
      LOGGER.error("Segment: {} of table: {} was not properly uploaded, replacing it", segmentName, tableNameWithType);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED, 1L);
    } else {
      // Another segment upload is in progress
      throw new ControllerApplicationException(LOGGER,
          String.format("Another segment upload is in progress for segment: %s of table: %s, retry later", segmentName,
              tableNameWithType), Response.Status.CONFLICT);
    }
  }

  private void processExistingSegment(String tableNameWithType, SegmentMetadata segmentMetadata,
      FileUploadType uploadType, ZNRecord existingSegmentMetadataZNRecord, @Nullable URI finalSegmentLocationURI,
      File segmentFile, @Nullable String sourceDownloadURIStr, String segmentDownloadURIStr,
      @Nullable String crypterName, long segmentSizeInBytes, boolean enableParallelPushProtection, HttpHeaders headers)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    int expectedVersion = existingSegmentMetadataZNRecord.getVersion();

    // Check if CRC match when IF-MATCH header is set
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(existingSegmentMetadataZNRecord);
    long existingCrc = segmentZKMetadata.getCrc();
    checkCRC(headers, tableNameWithType, segmentName, existingCrc);

    // Check segment upload start time when parallel push protection enabled
    if (enableParallelPushProtection) {
      // When segment upload start time is larger than 0, that means another upload is in progress
      long segmentUploadStartTime = segmentZKMetadata.getSegmentUploadStartTime();
      if (segmentUploadStartTime > 0) {
        handleParallelPush(tableNameWithType, segmentName, segmentUploadStartTime);
      }

      // Lock the segment by setting the upload start time in ZK
      segmentZKMetadata.setSegmentUploadStartTime(System.currentTimeMillis());
      if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata, expectedVersion)) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Failed to lock the segment: %s of table: %s, retry later", segmentName, tableNameWithType),
            Response.Status.CONFLICT);
      } else {
        // The version will increment if the zk metadata update is successful
        expectedVersion++;
      }
    }

    // Reset segment upload start time to unlock the segment later
    // NOTE: reset this value even if parallel push protection is not enabled so that segment can recover in case
    // previous segment upload did not finish properly and the parallel push protection is turned off
    segmentZKMetadata.setSegmentUploadStartTime(-1);

    try {
      // Construct the segment ZK metadata custom map modifier
      String customMapModifierStr =
          headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER);
      SegmentZKMetadataCustomMapModifier customMapModifier =
          customMapModifierStr != null ? new SegmentZKMetadataCustomMapModifier(customMapModifierStr) : null;

      // Update ZK metadata and refresh the segment if necessary
      long newCrc = Long.parseLong(segmentMetadata.getCrc());
      if (newCrc == existingCrc) {
        LOGGER.info(
            "New segment crc '{}' is the same as existing segment crc for segment '{}'. Updating ZK metadata without "
                + "refreshing the segment.", newCrc, segmentName);
        // NOTE: Even though we don't need to refresh the segment, we should still update the following fields:
        // - Creation time (not included in the crc)
        // - Refresh time
        // - Custom map
        segmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
        segmentZKMetadata.setRefreshTime(System.currentTimeMillis());
        if (customMapModifier != null) {
          segmentZKMetadata.setCustomMap(customMapModifier.modifyMap(segmentZKMetadata.getCustomMap()));
        } else {
          // If no modifier is provided, use the custom map from the segment metadata
          segmentZKMetadata.setCustomMap(segmentMetadata.getCustomMap());
        }
        if (!segmentZKMetadata.getDownloadUrl().equals(segmentDownloadURIStr)) {
          LOGGER.info("Updating segment download url from: {} to: {} even though crc is the same",
                  segmentZKMetadata.getDownloadUrl(), segmentDownloadURIStr);
          segmentZKMetadata.setDownloadUrl(segmentDownloadURIStr);
        }
        if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata, expectedVersion)) {
          throw new RuntimeException(
              String.format("Failed to update ZK metadata for segment: %s, table: %s, expected version: %d",
                  segmentName, tableNameWithType, expectedVersion));
        }
      } else {
        // New segment is different with the existing one, update ZK metadata and refresh the segment
        LOGGER.info(
            "New segment crc {} is different than the existing segment crc {}. Updating ZK metadata and refreshing "
                + "segment {}", newCrc, existingCrc, segmentName);
        if (finalSegmentLocationURI != null) {
          copySegmentToDeepStore(tableNameWithType, segmentName, uploadType, segmentFile, sourceDownloadURIStr,
              finalSegmentLocationURI);
        }

        // NOTE: Must first set the segment ZK metadata before trying to refresh because servers and brokers rely on
        // segment ZK metadata to refresh the segment (server will compare the segment ZK metadata with the local
        // metadata to decide whether to download the new segment; broker will update the segment partition info & time
        // boundary based on the segment ZK metadata)
        if (customMapModifier == null) {
          // If no modifier is provided, use the custom map from the segment metadata
          segmentZKMetadata.setCustomMap(null);
          ZKMetadataUtils.refreshSegmentZKMetadata(tableNameWithType, segmentZKMetadata, segmentMetadata,
              segmentDownloadURIStr, crypterName, segmentSizeInBytes);
        } else {
          // If modifier is provided, first set the custom map from the segment metadata, then apply the modifier
          ZKMetadataUtils.refreshSegmentZKMetadata(tableNameWithType, segmentZKMetadata, segmentMetadata,
              segmentDownloadURIStr, crypterName, segmentSizeInBytes);
          segmentZKMetadata.setCustomMap(customMapModifier.modifyMap(segmentZKMetadata.getCustomMap()));
        }
        if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata, expectedVersion)) {
          throw new RuntimeException(
              String.format("Failed to update ZK metadata for segment: %s, table: %s, expected version: %d",
                  segmentName, tableNameWithType, expectedVersion));
        }
        LOGGER.info("Updated segment: {} of table: {} to property store", segmentName, tableNameWithType);

        // Send a message to servers and brokers hosting the table to refresh the segment
        _pinotHelixResourceManager.sendSegmentRefreshMessage(tableNameWithType, segmentName, true, true);
      }
    } catch (Exception e) {
      if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata, expectedVersion)) {
        LOGGER.error("Failed to update ZK metadata for segment: {}, table: {}, expected version: {}", segmentName,
            tableNameWithType, expectedVersion);
      }
      throw e;
    }
  }

  private void checkCRC(HttpHeaders headers, String tableNameWithType, String segmentName, long existingCrc) {
    String expectedCrcStr = headers.getHeaderString(HttpHeaders.IF_MATCH);
    if (expectedCrcStr != null) {
      long expectedCrc;
      try {
        expectedCrc = Long.parseLong(expectedCrcStr);
      } catch (NumberFormatException e) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Caught exception for segment: %s of table: %s while parsing IF-MATCH CRC: \"%s\"",
                segmentName, tableNameWithType, expectedCrcStr), Response.Status.PRECONDITION_FAILED);
      }
      if (expectedCrc != existingCrc) {
        throw new ControllerApplicationException(LOGGER,
            String.format("For segment: %s of table: %s, expected CRC: %d does not match existing CRC: %d", segmentName,
                tableNameWithType, expectedCrc, existingCrc), Response.Status.PRECONDITION_FAILED);
      }
    }
  }

  private void processNewSegment(String tableNameWithType, SegmentMetadata segmentMetadata, FileUploadType uploadType,
      @Nullable URI finalSegmentLocationURI, File segmentFile, @Nullable String sourceDownloadURIStr,
      String segmentDownloadURIStr, @Nullable String crypterName, long segmentSizeInBytes,
      boolean enableParallelPushProtection, HttpHeaders headers)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    SegmentZKMetadata newSegmentZKMetadata;
    try {
      newSegmentZKMetadata =
          ZKMetadataUtils.createSegmentZKMetadata(tableNameWithType, segmentMetadata, segmentDownloadURIStr,
              crypterName, segmentSizeInBytes);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Got invalid segment metadata when adding segment: %s for table: %s, reason: %s", segmentName,
              tableNameWithType, e.getMessage()), Response.Status.BAD_REQUEST);
    }

    // Lock if enableParallelPushProtection is true.
    long segmentUploadStartTime = System.currentTimeMillis();
    if (enableParallelPushProtection) {
      newSegmentZKMetadata.setSegmentUploadStartTime(segmentUploadStartTime);
    }

    // Update zk metadata customer map
    String segmentZKMetadataCustomMapModifierStr = headers != null ? headers.getHeaderString(
        FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER) : null;
    if (segmentZKMetadataCustomMapModifierStr != null) {
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier =
          new SegmentZKMetadataCustomMapModifier(segmentZKMetadataCustomMapModifierStr);
      newSegmentZKMetadata.setCustomMap(
          segmentZKMetadataCustomMapModifier.modifyMap(newSegmentZKMetadata.getCustomMap()));
    }
    if (!_pinotHelixResourceManager.createSegmentZkMetadata(tableNameWithType, newSegmentZKMetadata)) {
      throw new RuntimeException(
          String.format("Failed to create ZK metadata for segment: %s of table: %s", segmentName, tableNameWithType));
    }

    if (finalSegmentLocationURI != null) {
      try {
        copySegmentToDeepStore(tableNameWithType, segmentName, uploadType, segmentFile, sourceDownloadURIStr,
            finalSegmentLocationURI);
      } catch (Exception e) {
        // Cleanup the Zk entry and the segment from the permanent directory if it exists.
        LOGGER.error("Could not move segment {} from table {} to permanent directory", segmentName, tableNameWithType,
            e);
        deleteSegmentIfNeeded(tableNameWithType, segmentName, segmentUploadStartTime, enableParallelPushProtection);
        throw e;
      }
    }

    try {
      _pinotHelixResourceManager.assignTableSegment(tableNameWithType, segmentMetadata.getName(), _controllerMetrics);
    } catch (Exception e) {
      // assignTableSegment removes the zk entry.
      // Call deleteSegment to remove the segment from permanent location if needed.
      LOGGER.error("Caught exception while calling assignTableSegment for adding segment: {} to table: {}", segmentName,
          tableNameWithType, e);
      deleteSegmentIfNeeded(tableNameWithType, segmentName, segmentUploadStartTime, enableParallelPushProtection);
      throw e;
    }

    if (enableParallelPushProtection) {
      // Release lock. Expected version will be 0 as we hold a lock and no updates could take place meanwhile.
      newSegmentZKMetadata.setSegmentUploadStartTime(-1);
      if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, newSegmentZKMetadata, 0)) {
        // There is a race condition when it took too much time for the 1st segment upload to process (due to slow
        // PinotFS access), which leads to the 2nd attempt of segment upload, and the 2nd segment upload succeeded.
        // In this case, when the 1st upload comes back, it shouldn't blindly delete the segment when it failed to
        // update the zk metadata. Instead, the 1st attempt should validate the upload start time one more time. If the
        // start time doesn't match with the one persisted in zk metadata, segment deletion should be skipped.
        String errorMsg =
            String.format("Failed to update ZK metadata for segment: %s of table: %s", segmentFile, tableNameWithType);
        LOGGER.error(errorMsg);
        deleteSegmentIfNeeded(tableNameWithType, segmentName, segmentUploadStartTime, true);
        throw new RuntimeException(errorMsg);
      }
    }
  }

  /**
   * Deletes the segment to be uploaded if either one of the criteria is qualified:
   * 1) the uploadStartTime matches with the one persisted in ZK metadata.
   * 2) enableParallelPushProtection is not enabled.
   */
  private void deleteSegmentIfNeeded(String tableNameWithType, String segmentName, long currentSegmentUploadStartTime,
      boolean enableParallelPushProtection) {
    ZNRecord existingSegmentMetadataZNRecord =
        _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    if (existingSegmentMetadataZNRecord == null) {
      return;
    }
    // Check if the upload start time is set by this thread itself, if yes delete the segment.
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(existingSegmentMetadataZNRecord);
    long existingSegmentUploadStartTime = segmentZKMetadata.getSegmentUploadStartTime();
    LOGGER.info("Parallel push protection is {} for segment: {}.",
        (enableParallelPushProtection ? "enabled" : "disabled"), segmentName);
    if (!enableParallelPushProtection || currentSegmentUploadStartTime == existingSegmentUploadStartTime) {
      _pinotHelixResourceManager.deleteSegment(tableNameWithType, segmentName);
      LOGGER.info("Deleted zk entry and segment {} for table {}.", segmentName, tableNameWithType);
    }
  }

  private void copySegmentToDeepStore(String tableNameWithType, String segmentName, FileUploadType uploadType,
      File segmentFile, String sourceDownloadURIStr, URI finalSegmentLocationURI)
      throws Exception {
    if (uploadType == FileUploadType.METADATA) {
      // In Metadata push, local segmentFile only contains metadata.
      // Copy segment over from sourceDownloadURI to final location.
      copyFromSegmentURIToDeepStore(new URI(sourceDownloadURIStr), finalSegmentLocationURI);
      LOGGER.info("Copied segment: {} of table: {} to final location: {}", segmentName, tableNameWithType,
          finalSegmentLocationURI);
    } else {
      // In push types other than METADATA, local segmentFile contains the complete segment.
      // Move local segment to final location
      copyFromSegmentFileToDeepStore(segmentFile, finalSegmentLocationURI);
      LOGGER.info("Copied segment: {} of table: {} to final location: {}", segmentName, tableNameWithType,
          finalSegmentLocationURI);
    }
  }

  private void copyFromSegmentFileToDeepStore(File segmentFile, URI finalSegmentLocationURI)
      throws Exception {
    LOGGER.info("Copying segment from: {} to: {}", segmentFile.getAbsolutePath(), finalSegmentLocationURI);
    PinotFSFactory.create(finalSegmentLocationURI.getScheme()).copyFromLocalFile(segmentFile, finalSegmentLocationURI);
  }

  private void copyFromSegmentURIToDeepStore(URI sourceDownloadURI, URI finalSegmentLocationURI)
      throws Exception {
    if (sourceDownloadURI.equals(finalSegmentLocationURI)) {
      LOGGER.info("Skip copying segment as sourceDownloadURI: {} is the same as finalSegmentLocationURI",
          sourceDownloadURI);
    } else {
      Preconditions.checkState(sourceDownloadURI.getScheme().equals(finalSegmentLocationURI.getScheme()));
      LOGGER.info("Copying segment from: {} to: {}", sourceDownloadURI, finalSegmentLocationURI);
      PinotFSFactory.create(finalSegmentLocationURI.getScheme()).copy(sourceDownloadURI, finalSegmentLocationURI);
    }
  }
}
