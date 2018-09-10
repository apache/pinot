/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.upload;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.resources.ControllerApplicationException;
import com.linkedin.pinot.controller.api.resources.FileUploadPathProvider;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.net.URI;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ZKOperator is a util class that is used during segment upload to set relevant metadata fields in zk. It will currently
 * also perform the data move. In the future when we introduce versioning, we will decouple these two steps.
 */
public class ZKOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZKOperator.class);
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private ControllerConf _controllerConf;
  private ControllerMetrics _controllerMetrics;

  public ZKOperator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
  }

  public void completeSegmentOperations(SegmentMetadata segmentMetadata, URI finalSegmentLocationURI, File currentSegmentLocation, boolean enableParallelPushProtection, HttpHeaders headers,
      FileUploadPathProvider provider) throws Exception {
    String rawTableName = segmentMetadata.getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String segmentName = segmentMetadata.getName();

    ZNRecord znRecord = _pinotHelixResourceManager.getSegmentMetadataZnRecord(offlineTableName, segmentName);

    // Brand new segment, not refresh, directly add the segment
    if (znRecord == null) {
      LOGGER.info("Adding new segment: {}", segmentName);
      try {
        moveSegmentToPermanentDirectory(currentSegmentLocation, finalSegmentLocationURI);
        LOGGER.info("Moved segment {} from temp location {} to {}", segmentName, currentSegmentLocation.getAbsolutePath(), finalSegmentLocationURI.getPath());
      } catch (Exception e) {
        LOGGER.error("Could not move segment {} from table {} to permanent directory", segmentName, rawTableName);
        throw new RuntimeException(e);
      }
      if (finalSegmentLocationURI.getScheme().equals("file")) {
        String downloadUrl = ControllerConf.constructDownloadUrl(rawTableName, segmentName, provider.getVip());
        _pinotHelixResourceManager.addNewSegment(segmentMetadata, downloadUrl);

      } else {
        _pinotHelixResourceManager.addNewSegment(segmentMetadata, finalSegmentLocationURI.toString());
      }
      return;
    }

    LOGGER.info("Segment {} already exists, refreshing if necessary", segmentName);

    OfflineSegmentZKMetadata existingSegmentZKMetadata = new OfflineSegmentZKMetadata(znRecord);
    long existingCrc = existingSegmentZKMetadata.getCrc();

    // Check if CRC match when IF-MATCH header is set
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

    // Check segment upload start time when parallel push protection enabled
    if (enableParallelPushProtection) {
      // When segment upload start time is larger than 0, that means another upload is in progress
      long segmentUploadStartTime = existingSegmentZKMetadata.getSegmentUploadStartTime();
      if (segmentUploadStartTime > 0) {
        if (System.currentTimeMillis() - segmentUploadStartTime > _controllerConf.getSegmentUploadTimeoutInMillis()) {
          // Last segment upload does not finish properly, replace the segment
          LOGGER.error("Segment: {} of table: {} was not properly uploaded, replacing it", segmentName,
              offlineTableName);
          _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED, 1L);
        } else {
          // Another segment upload is in progress
          throw new ControllerApplicationException(LOGGER,
              "Another segment upload is in progress for segment: " + segmentName + " of table: " + offlineTableName
                  + ", retry later", Response.Status.CONFLICT);
        }
      }

      // Lock the segment by setting the upload start time in ZK
      existingSegmentZKMetadata.setSegmentUploadStartTime(System.currentTimeMillis());
      if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata, znRecord.getVersion())) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to lock the segment: " + segmentName + " of table: " + offlineTableName + ", retry later",
            Response.Status.CONFLICT);
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
      existingSegmentZKMetadata.setCustomMap(
          segmentZKMetadataCustomMapModifier.modifyMap(existingSegmentZKMetadata.getCustomMap()));

      // Update ZK metadata and refresh the segment if necessary
      long newCrc = Long.valueOf(segmentMetadata.getCrc());
      if (newCrc == existingCrc) {
        LOGGER.info("New segment crc {} is same as existing segment crc {} for segment {}. Updating ZK metadata without refreshing the segment {}",
            newCrc, existingCrc, segmentName);
        if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
          throw new RuntimeException(
              "Failed to update ZK metadata for segment: " + segmentName + " of table: " + offlineTableName);
        }
      } else {
        // New segment is different with the existing one, update ZK metadata and refresh the segment
        LOGGER.info("New segment crc {} is different than the existing segment crc {}. Updating ZK metadata and refreshing segment {}",
            newCrc, existingCrc, segmentName);
        moveSegmentToPermanentDirectory(currentSegmentLocation, finalSegmentLocationURI);
        LOGGER.info("Moved segment {} from temp location {} to {}", segmentName, currentSegmentLocation.getAbsolutePath(), finalSegmentLocationURI.getPath());
        if (finalSegmentLocationURI.getScheme().equals("file")) {
          String downloadUrl = ControllerConf.constructDownloadUrl(rawTableName, segmentName, provider.getVip());
          _pinotHelixResourceManager.refreshSegment(segmentMetadata, existingSegmentZKMetadata, downloadUrl);

        } else {
          _pinotHelixResourceManager.refreshSegment(segmentMetadata, existingSegmentZKMetadata, finalSegmentLocationURI.toString());
        }
      }
    } catch (Exception e) {
      if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
        LOGGER.error("Failed to update ZK metadata for segment: {} of table: {}", segmentName, offlineTableName);
      }
      throw e;
    }
  }

  private void moveSegmentToPermanentDirectory(File currentSegmentLocation, URI finalSegmentLocationURI) throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(finalSegmentLocationURI.getScheme());

    // Overwrite current segment file
    if (pinotFS.exists(finalSegmentLocationURI)) {
      pinotFS.delete(finalSegmentLocationURI);
    }
    pinotFS.copyFromLocalFile(currentSegmentLocation, finalSegmentLocationURI);
  }
}
