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
package com.linkedin.pinot.tools.backfill;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.SimpleHttpResponse;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains APIs which are used for backfilling the pinot segments with dateTimeFieldSpec
 */
public class BackfillSegmentUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(BackfillSegmentUtils.class);


  private static final String SEGMENTS_ENDPOINT = "%s/segments/%s";
  private static final String DOWNLOAD_SEGMENT_ENDPOINT = "%s/segments/%s/%s";
  public static final String TAR_SUFFIX = ".tar.gz";

  private HttpHost _controllerHttpHost;
  private String _controllerHost;
  private String _controllerPort;

  public BackfillSegmentUtils(String controllerHost, String controllerPort) {
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
   _controllerHttpHost = new HttpHost(controllerHost, Integer.valueOf(controllerPort));
  }


  /**
   * Fetches the list of all segment names for a table
   * @param tableName
   * @return
   * @throws IOException
   */
  public List<String> getAllSegments(String tableName, SegmentType segmentType) throws IOException {

    List<String> allSegments = new ArrayList<>();
    String urlString = String.format(SEGMENTS_ENDPOINT, _controllerHttpHost.toURI(), tableName);
    URL url = new URL(urlString);
    InputStream is = url.openConnection().getInputStream();

    String response = IOUtils.toString(is);
    JsonNode segmentsData = new ObjectMapper().readTree(response);

    if (segmentsData != null) {
      if (segmentType == null || SegmentType.OFFLINE.equals(segmentType)) {
        JsonNode offlineSegments = segmentsData.get(0).get(SegmentType.OFFLINE.toString());
        if (offlineSegments != null) {
          for (JsonNode segment : offlineSegments) {
            allSegments.add(segment.asText());
          }
        }
      }
      if (segmentType == null || SegmentType.REALTIME.equals(segmentType)) {
        JsonNode realtimeSegments = segmentsData.get(0).get(SegmentType.REALTIME.toString());
        if (realtimeSegments != null) {
          for (JsonNode segment : realtimeSegments) {
            allSegments.add(segment.asText());
          }
        }
      }
    }
    LOGGER.info("All segments : {}", allSegments);

    return allSegments;
  }


  /**
   * Downloads a segment from a table to a directory locally, and backs it up to given backup path
   * @param tableName
   * @param segmentName
   * @param downloadSegmentDir - download segment path
   * @param tableBackupDir - backup segments path
   * @return
   */
  public boolean downloadSegment(String tableName, String segmentName, File downloadSegmentDir, File tableBackupDir)  {
    boolean downloadSuccess = true;
    if (downloadSegmentDir.exists()) {
      try {
        FileUtils.deleteDirectory(downloadSegmentDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete directory {}", downloadSegmentDir, e);
      }
    }
    downloadSegmentDir.mkdirs();

    try {
      String urlString = String.format(DOWNLOAD_SEGMENT_ENDPOINT, _controllerHttpHost.toURI(), tableName, segmentName);
      URL url = new URL(urlString);
      InputStream inputStream = url.openConnection().getInputStream();

      File segmentTar = new File(downloadSegmentDir, segmentName + TAR_SUFFIX);
      LOGGER.info("Downloading {} to {}", segmentName, segmentTar);
      OutputStream outputStream = new FileOutputStream(segmentTar);

      IOUtils.copyLarge(inputStream, outputStream);
      if (!segmentTar.exists()) {
        LOGGER.error("Download of {} unsuccessful", segmentName);
        return false;
      }

      LOGGER.info("Backing up segment {} to {}", segmentTar, tableBackupDir);
      FileUtils.copyFileToDirectory(segmentTar, tableBackupDir);

      LOGGER.info("Extracting segment {} to {}", segmentTar, downloadSegmentDir);
      TarGzCompressionUtils.unTar(segmentTar, downloadSegmentDir);

      File segmentDir = new File(downloadSegmentDir, segmentName);
      if (!segmentDir.exists()) {
        throw new RuntimeException("Unable to untar segment " + segmentName);
      } else {
        FileUtils.deleteQuietly(segmentTar);
      }
    } catch (Exception e) {
      LOGGER.error("Error in downloading segment {}", segmentName, e);
      downloadSuccess = false;
    }

    return downloadSuccess;
  }

  /**
   * Uploads the segment tar to the controller.
   */
  public boolean uploadSegment(String segmentName, File segmentDir, File outputDir) {
    boolean success = true;

    File segmentTar = new File(outputDir, segmentName + TAR_SUFFIX);

    try {
      TarGzCompressionUtils.createTarGzOfDirectory(segmentDir.getAbsolutePath(), segmentTar.getAbsolutePath());
      LOGGER.info("Created tar of {} at {}", segmentDir.getAbsolutePath(), segmentTar.getAbsolutePath());
      try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
        SimpleHttpResponse response = fileUploadDownloadClient.uploadSegment(
            FileUploadDownloadClient.getUploadSegmentHttpURI(_controllerHost, Integer.parseInt(_controllerPort)),
            segmentName, segmentTar);
        int statusCode = response.getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
          success = false;
        }
        LOGGER.info("Uploaded segment: {} and got response {}: {}", segmentName, statusCode, response.getResponse());
      }
    } catch (Exception e) {
      LOGGER.error("Exception in segment upload {}", segmentTar, e);
      success = false;
    }
    return success;
  }
}
