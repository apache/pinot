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
package com.linkedin.pinot.controller.api.storage;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.resources.FileUploadPathProvider;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.net.URI;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.ZNRecord;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains upload-specific methods
 */
public class PinotSegmentUploadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadUtils.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;


  public URI constructFinalLocation(SegmentMetadata segmentMetadata, UUID version) throws Exception {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
    return new File(new File(new File(provider.getBaseDataDir(), segmentMetadata.getTableName()), segmentMetadata.getName()), version.toString()).toURI();
  }
  /**
   * Updates zk metadata for uploaded segments
   * @param segmentMetadata
   * @param segmentUploaderConfig
   */
  public void pushMetadata(SegmentMetadata segmentMetadata, SegmentUploaderConfig segmentUploaderConfig)
      throws JSONException {

    logIncomingSegmentInformation(segmentUploaderConfig.getHeaders());
    UUID version = UUID.randomUUID();
    PinotFS pinotFS;
    URI srcUri;
    URI dstUri;
    try {
      srcUri = new URI(segmentUploaderConfig.getHeaders().getHeaderString(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI));
      pinotFS = PinotFSFactory.getInstance().init(_controllerConf, srcUri);
      dstUri = constructFinalLocation(segmentMetadata, version);
      pinotFS.copy(srcUri, dstUri);
    } catch (Exception e) {
      LOGGER.error("Could not copy file to final directory");
      throw new RuntimeException(e);
    }

    HttpHeaders headers = segmentUploaderConfig.getHeaders();
    String tableName = segmentMetadata.getTableName();
    String segmentName = segmentMetadata.getName();
    String tableNameWithType = getTableNameWithType(segmentName, tableName);

    ZNRecord znRecord = _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);

    // Brand new segment, not refresh, directly add the segment
    if (znRecord == null) {
      _pinotHelixResourceManager.addNewSegment(segmentMetadata, dstUri.getPath());
      return;
    }

    // Segment already exists, refresh if necessary
    OfflineSegmentZKMetadata existingSegmentZKMetadata = new OfflineSegmentZKMetadata(znRecord);
    long existingCrc = existingSegmentZKMetadata.getCrc();

    try {
      // Modify the custom map in segment ZK metadata
      String segmentZKMetadataCustomMapModifierStr = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER);
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
        // New segment is the same as the existing one, only update ZK metadata without refresh the segment
        if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
          throw new RuntimeException(
              "Failed to update ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
        }
      } else {
        _pinotHelixResourceManager.refreshSegment(segmentMetadata, existingSegmentZKMetadata, dstUri.getPath());
      }
    } catch (Exception e) {
      throw e;
    }
  }

  private void logIncomingSegmentInformation(HttpHeaders headers) {
    if (headers != null) {
      // TODO: Add these headers into open source hadoop jobs
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER));
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.TABLE_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER));
    }
  }

  private String getTableNameWithType(String segmentName, String tableName) {
    if (SegmentName.getSegmentType(segmentName).equals(SegmentName.RealtimeSegmentType.UNSUPPORTED)) {
      // Offline segment
      return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    } else {
      return TableNameBuilder.REALTIME.tableNameWithType(tableName);
    }
  }

}
