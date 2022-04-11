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

import java.io.File;
import java.net.URI;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ZKOperator is a util class that is used during segment upload to set relevant metadata fields in zk. It will
 * currently
 * also perform the data move. In the future when we introduce versioning, we will decouple these two steps.
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
      URI finalSegmentLocationURI, File currentSegmentLocation, boolean enableParallelPushProtection,
      HttpHeaders headers, String zkDownloadURI, boolean moveSegmentToFinalLocation, String crypter,
      boolean allowRefresh, long segmentSizeInBytes)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    ZNRecord segmentMetadataZNRecord =
        _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    boolean refreshOnly =
        Boolean.parseBoolean(headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY));
    if (segmentMetadataZNRecord == null) {
      if (refreshOnly) {
        throw new ControllerApplicationException(LOGGER,
            "Cannot refresh non-existing segment, aborted uploading segment: " + segmentName + " of table: "
                + tableNameWithType, Response.Status.GONE);
      }
      LOGGER.info("Adding new segment {} from table {}", segmentName, tableNameWithType);
      processNewSegment(segmentMetadata, finalSegmentLocationURI, currentSegmentLocation, zkDownloadURI, headers,
          crypter, tableNameWithType, segmentName, moveSegmentToFinalLocation, enableParallelPushProtection,
          segmentSizeInBytes);
      return;
    }

    // We reach here if a segment with the same name already exists.

    if (!allowRefresh) {
      // We cannot perform this check up-front in UploadSegment API call. If a segment doesn't exist during the check
      // done up-front but ends up getting created before the check here, we could incorrectly refresh an existing
      // segment.
      throw new ControllerApplicationException(LOGGER,
          "Segment: " + segmentName + " already exists in table: " + tableNameWithType + ". Refresh not permitted.",
          Response.Status.CONFLICT);
    }

    // TODO Allow segment refreshing for realtime tables.
    if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      throw new ControllerApplicationException(LOGGER,
          "Refresh existing segment " + segmentName + " for realtime table " + tableNameWithType
              + " is not yet supported ", Response.Status.NOT_IMPLEMENTED);
    }

    LOGGER.info("Segment {} from table {} already exists, refreshing if necessary", segmentName, tableNameWithType);
    processExistingSegment(segmentMetadata, finalSegmentLocationURI, currentSegmentLocation,
        enableParallelPushProtection, headers, zkDownloadURI, crypter, tableNameWithType, segmentName,
        segmentMetadataZNRecord, moveSegmentToFinalLocation, segmentSizeInBytes);
  }

  private void processExistingSegment(SegmentMetadata segmentMetadata, URI finalSegmentLocationURI,
      File currentSegmentLocation, boolean enableParallelPushProtection, HttpHeaders headers, String zkDownloadURI,
      String crypter, String tableNameWithType, String segmentName, ZNRecord znRecord,
      boolean moveSegmentToFinalLocation, long segmentSizeInBytes)
      throws Exception {

    SegmentZKMetadata existingSegmentZKMetadata = new SegmentZKMetadata(znRecord);
    long existingCrc = existingSegmentZKMetadata.getCrc();
    int expectedVersion = znRecord.getVersion();

    // Check if CRC match when IF-MATCH header is set
    checkCRC(headers, tableNameWithType, segmentName, existingCrc);

    // Check segment upload start time when parallel push protection enabled
    if (enableParallelPushProtection) {
      // When segment upload start time is larger than 0, that means another upload is in progress
      long segmentUploadStartTime = existingSegmentZKMetadata.getSegmentUploadStartTime();
      if (segmentUploadStartTime > 0) {
        if (System.currentTimeMillis() - segmentUploadStartTime > _controllerConf.getSegmentUploadTimeoutInMillis()) {
          // Last segment upload does not finish properly, replace the segment
          LOGGER.error("Segment: {} of table: {} was not properly uploaded, replacing it", segmentName,
              tableNameWithType);
          _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED, 1L);
        } else {
          // Another segment upload is in progress
          throw new ControllerApplicationException(LOGGER,
              "Another segment upload is in progress for segment: " + segmentName + " of table: " + tableNameWithType
                  + ", retry later", Response.Status.CONFLICT);
        }
      }

      // Lock the segment by setting the upload start time in ZK
      existingSegmentZKMetadata.setSegmentUploadStartTime(System.currentTimeMillis());
      if (!_pinotHelixResourceManager
          .updateZkMetadata(tableNameWithType, existingSegmentZKMetadata, expectedVersion)) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to lock the segment: " + segmentName + " of table: " + tableNameWithType + ", retry later",
            Response.Status.CONFLICT);
      } else {
        // The version will increment if the zk metadata update is successful
        expectedVersion++;
      }
    }

    // Reset segment upload start time to unlock the segment later
    // NOTE: reset this value even if parallel push protection is not enabled so that segment can recover in case
    // previous segment upload did not finish properly and the parallel push protection is turned off
    existingSegmentZKMetadata.setSegmentUploadStartTime(-1);

    try {
      // Modify the custom map in segment ZK metadata
      String segmentZKMetadataCustomMapModifierStr =
          headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER);
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier;
      if (segmentZKMetadataCustomMapModifierStr != null) {
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(segmentZKMetadataCustomMapModifierStr);
      } else {
        // By default, use REPLACE modify mode
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.REPLACE, null);
      }
      existingSegmentZKMetadata
          .setCustomMap(segmentZKMetadataCustomMapModifier.modifyMap(existingSegmentZKMetadata.getCustomMap()));

      // Update ZK metadata and refresh the segment if necessary
      long newCrc = Long.valueOf(segmentMetadata.getCrc());
      if (newCrc == existingCrc) {
        LOGGER.info(
            "New segment crc '{}' is the same as existing segment crc for segment '{}'. Updating ZK metadata without "
                + "refreshing the segment.", newCrc, segmentName);
        // NOTE: even though we don't need to refresh the segment, we should still update creation time and refresh time
        // (creation time is not included in the crc)
        existingSegmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
        existingSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
        // NOTE: in rare cases the segment can be deleted before the metadata is updated and the expected version won't
        // match, we should fail the request for such cases
        if (!_pinotHelixResourceManager
            .updateZkMetadata(tableNameWithType, existingSegmentZKMetadata, expectedVersion)) {
          throw new RuntimeException(
              "Failed to update ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
        }
      } else {
        // New segment is different with the existing one, update ZK metadata and refresh the segment
        LOGGER.info(
            "New segment crc {} is different than the existing segment crc {}. Updating ZK metadata and refreshing "
                + "segment {}", newCrc, existingCrc, segmentName);
        if (moveSegmentToFinalLocation) {
          moveSegmentToPermanentDirectory(currentSegmentLocation, finalSegmentLocationURI);
          LOGGER.info("Moved segment {} from temp location {} to {}", segmentName,
              currentSegmentLocation.getAbsolutePath(), finalSegmentLocationURI.getPath());
        } else {
          LOGGER.info("Skipping segment move, keeping segment {} from table {} at {}", segmentName, tableNameWithType,
              zkDownloadURI);
        }

        _pinotHelixResourceManager
            .refreshSegment(tableNameWithType, segmentMetadata, existingSegmentZKMetadata, expectedVersion,
                zkDownloadURI, crypter, segmentSizeInBytes);
      }
    } catch (Exception e) {
      if (!_pinotHelixResourceManager
          .updateZkMetadata(tableNameWithType, existingSegmentZKMetadata, expectedVersion)) {
        LOGGER.error("Failed to update ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      }
      throw e;
    }
  }

  private void checkCRC(HttpHeaders headers, String offlineTableName, String segmentName, long existingCrc) {
    String expectedCrcStr = headers.getHeaderString(HttpHeaders.IF_MATCH);
    if (expectedCrcStr != null) {
      long expectedCrc;
      try {
        expectedCrc = Long.parseLong(expectedCrcStr);
      } catch (NumberFormatException e) {
        throw new ControllerApplicationException(LOGGER,
            "Caught exception for segment: " + segmentName + " of table: " + offlineTableName
                + " while parsing IF-MATCH CRC: \"" + expectedCrcStr + "\"", Response.Status.PRECONDITION_FAILED);
      }
      if (expectedCrc != existingCrc) {
        throw new ControllerApplicationException(LOGGER,
            "For segment: " + segmentName + " of table: " + offlineTableName + ", expected CRC: " + expectedCrc
                + " does not match existing CRC: " + existingCrc, Response.Status.PRECONDITION_FAILED);
      }
    }
  }

  private void processNewSegment(SegmentMetadata segmentMetadata, URI finalSegmentLocationURI,
      File currentSegmentLocation, String zkDownloadURI, HttpHeaders headers, String crypter, String tableNameWithType,
      String segmentName, boolean moveSegmentToFinalLocation, boolean enableParallelPushProtection,
      long segmentSizeInBytes)
      throws Exception {
    SegmentZKMetadata newSegmentZKMetadata =
        _pinotHelixResourceManager.constructZkMetadataForNewSegment(tableNameWithType, segmentMetadata, zkDownloadURI,
            crypter, segmentSizeInBytes);

    // Lock if enableParallelPushProtection is true.
    if (enableParallelPushProtection) {
      newSegmentZKMetadata.setSegmentUploadStartTime(System.currentTimeMillis());
    }

    // Update zk metadata customer map
    String segmentZKMetadataCustomMapModifierStr = headers != null ? headers
        .getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER) : null;
    if (segmentZKMetadataCustomMapModifierStr != null) {
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier =
          new SegmentZKMetadataCustomMapModifier(segmentZKMetadataCustomMapModifierStr);
      newSegmentZKMetadata
          .setCustomMap(segmentZKMetadataCustomMapModifier.modifyMap(newSegmentZKMetadata.getCustomMap()));
    }
    if (!_pinotHelixResourceManager.createSegmentZkMetadata(tableNameWithType, newSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to create ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
    }

    // For v1 segment uploads, we will not move the segment
    if (moveSegmentToFinalLocation) {
      try {
        moveSegmentToPermanentDirectory(currentSegmentLocation, finalSegmentLocationURI);
        LOGGER
            .info("Moved segment {} from temp location {} to {}", segmentName, currentSegmentLocation.getAbsolutePath(),
                finalSegmentLocationURI.getPath());
      } catch (Exception e) {
        // Cleanup the Zk entry and the segment from the permanent directory if it exists.
        LOGGER
            .error("Could not move segment {} from table {} to permanent directory", segmentName, tableNameWithType, e);
        _pinotHelixResourceManager.deleteSegment(tableNameWithType, segmentName);
        LOGGER.info("Deleted zk entry and segment {} for table {}.", segmentName, tableNameWithType);
        throw e;
      }
    } else {
      LOGGER.info("Skipping segment move, keeping segment {} from table {} at {}", segmentName, tableNameWithType,
          zkDownloadURI);
    }

    try {
      _pinotHelixResourceManager.assignTableSegment(tableNameWithType, segmentMetadata.getName());
    } catch (Exception e) {
      // assignTableSegment removes the zk entry. Call deleteSegment to remove the segment from permanent location.
      LOGGER
          .error("Caught exception while calling assignTableSegment for adding segment: {} to table: {}", segmentName,
              tableNameWithType, e);
      _pinotHelixResourceManager.deleteSegment(tableNameWithType, segmentName);
      LOGGER.info("Deleted zk entry and segment {} for table {}.", segmentName, tableNameWithType);
      throw e;
    }

    if (enableParallelPushProtection) {
      // Release lock. Expected version will be 0 as we hold a lock and no updates could take place meanwhile.
      newSegmentZKMetadata.setSegmentUploadStartTime(-1);
      if (!_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, newSegmentZKMetadata, 0)) {
        _pinotHelixResourceManager.deleteSegment(tableNameWithType, segmentName);
        LOGGER.info("Deleted zk entry and segment {} for table {}.", segmentName, tableNameWithType);
        throw new RuntimeException(
            "Failed to update ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
      }
    }
  }

  private void moveSegmentToPermanentDirectory(File currentSegmentLocation, URI finalSegmentLocationURI)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(finalSegmentLocationURI.getScheme());

    // Overwrites current segment file
    LOGGER.info("Copying segment from {} to {}", currentSegmentLocation.getAbsolutePath(),
        finalSegmentLocationURI.toString());
    pinotFS.copyFromLocalFile(currentSegmentLocation, finalSegmentLocationURI);
  }
}
