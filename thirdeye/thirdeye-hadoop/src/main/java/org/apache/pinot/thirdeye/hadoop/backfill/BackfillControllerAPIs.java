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
package com.linkedin.thirdeye.hadoop.backfill;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;

/**
 * Contains APIs which are used for backfilling the pinot segments with star tree index
 */
public class BackfillControllerAPIs {

  private static Logger LOGGER = LoggerFactory.getLogger(BackfillControllerAPIs.class);
  private HttpHost controllerHttpHost;
  private String tableName;

  private static String SEGMENTS_ENDPOINT = "segments/";
  private static String TABLES_ENDPOINT = "tables/";
  private static String METADATA_ENDPOINT = "metadata";
  private static String UTF_8 = "UTF-8";

  private static String SEGMENT_NAME = "segment.name";
  private static String SEGMENT_TABLE_NAME = "segment.table.name";
  private static String SEGMENT_END_TIME = "segment.end.time";
  private static String SEGMENT_START_TIME = "segment.start.time";
  private static String SEGMENT_TIME_UNIT = "segment.time.unit";

  BackfillControllerAPIs(String controllerHost, int controllerPort, String tableName) {
    this.tableName = tableName;
    LOGGER.info("Connecting to {} {} table {}", controllerHost, controllerPort, tableName);
    controllerHttpHost = new HttpHost(controllerHost, controllerPort);
  }

  /**
   * Downloads a segment from the controller, given the table name and segment name
   * @param segmentName
   * @param hdfsSegmentPath
   * @throws IOException
   * @throws ArchiveException
   */
  public void downloadSegment(String segmentName, Path hdfsSegmentPath)
      throws IOException, ArchiveException {

    FileSystem fs = FileSystem.get(new Configuration());
    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet(SEGMENTS_ENDPOINT + URLEncoder.encode(tableName, UTF_8)
        + "/" + URLEncoder.encode(segmentName, UTF_8));
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      LOGGER.info("Fetching segment {}", segmentName);
      InputStream content = res.getEntity().getContent();

      File tempDir = new File(Files.createTempDir(), "thirdeye_temp");
      tempDir.mkdir();
      LOGGER.info("Creating temporary dir for staging segments {}", tempDir);
      File tempSegmentDir = new File(tempDir, segmentName);
      File tempSegmentTar = new File(tempDir, segmentName + ThirdEyeConstants.TAR_SUFFIX);

      LOGGER.info("Downloading {} to {}", segmentName, tempSegmentTar);
      OutputStream out = new FileOutputStream(tempSegmentTar);
      IOUtils.copy(content, out);
      if (!tempSegmentTar.exists()) {
        throw new IllegalStateException("Download of " + segmentName + " unsuccessful");
      }

      LOGGER.info("Extracting segment {} to {}", tempSegmentTar, tempDir);
      TarGzCompressionUtils.unTar(tempSegmentTar, tempDir);
      File[] files = tempDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return !name.endsWith(ThirdEyeConstants.TAR_SUFFIX) && new File(dir, name).isDirectory();
        }
      });
      if (files.length == 0) {
        throw new IllegalStateException("Failed to extract " + tempSegmentTar + " to " + tempDir);
      } else if (!files[0].getName().equals(tempSegmentDir.getName())){
        LOGGER.info("Moving extracted segment to the segment dir {}", tempSegmentDir);
        FileUtils.moveDirectory(files[0], tempSegmentDir);
      }
      if (!tempSegmentDir.exists()) {
        throw new IllegalStateException("Failed to move " + files[0] + " to " + tempSegmentDir);
      }

      LOGGER.info("Copying segment from {} to hdfs {}", tempSegmentDir, hdfsSegmentPath);
      fs.copyFromLocalFile(new Path(tempSegmentDir.toString()), hdfsSegmentPath);
      Path hdfsSegmentDir = new Path(hdfsSegmentPath, segmentName);
      if (!fs.exists(hdfsSegmentDir)) {
        throw new IllegalStateException("Failed to copy segment " + segmentName + " from local path " + tempSegmentDir
            + " to hdfs path " + hdfsSegmentPath);
      }
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    LOGGER.info("Successfully downloaded segment {} to {}", segmentName, hdfsSegmentPath);
  }

  /**
   * Given a time range and list of all segments for a table, returns all segments which are in the time range
   * @param tableName
   * @param allSegments
   * @param startTime
   * @param endTime
   * @return
   * @throws Exception
   */
  public List<String> findSegmentsInRange(String tableName, List<String> allSegments, long startTime, long endTime)
      throws Exception {
    List<String> segmentsInRange = new ArrayList<>();
    for (String segmentName : allSegments) {
      Map<String, String> metadata = getSegmentMetadata(tableName, segmentName);
      long segmentStartTime = Long.valueOf(metadata.get(SEGMENT_START_TIME));
      long segmentEndTime = Long.valueOf(metadata.get(SEGMENT_END_TIME));
      String segmentTableName = metadata.get(SEGMENT_TABLE_NAME);

      // TODO:
      // Using time value directly for now, as we only have time unit and not time size in metadata
      // Once we have time size in metadata, we can accept the time in millis and then convert time from metadata accordingly
      if (segmentTableName.equals(tableName) && ((segmentStartTime >= startTime && segmentStartTime <= endTime)
          || (segmentEndTime >= startTime && segmentEndTime <= endTime))) {
        LOGGER.info("Segment name : {}, Segment start : {}, Segment end : {}, Segment table : {}",
            segmentName, segmentStartTime, segmentEndTime, segmentTableName);
        segmentsInRange.add(segmentName);
      }
    }
    return segmentsInRange;
  }

  /**
   * Fetches the list of all segment names for a table
   * @param tableName
   * @return
   * @throws IOException
   */
  public List<String> getAllSegments(String tableName) throws IOException {
    List<String> allSegments = new ArrayList<>();

    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet(SEGMENTS_ENDPOINT + URLEncoder.encode(tableName, UTF_8));
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      String response = IOUtils.toString(content);
      List<String> allSegmentsPaths = getSegmentsFromResponse(response);
      for (String segment : allSegmentsPaths) {
        allSegments.add(segment.substring(segment.lastIndexOf("/") + 1));
      }
      LOGGER.info("All segments : {}", allSegments);
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return allSegments;
  }

  /**
   * Returns the metadata of a segment, given the segment name and table name
   * @param tableName - table where segment resides
   * @param segmentName - name of the segment
   * @return
   * @throws IOException
   */
  public Map<String, String> getSegmentMetadata(String tableName, String segmentName) throws IOException {
    Map<String, String> metadata = null;
    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet(TABLES_ENDPOINT + URLEncoder.encode(tableName, UTF_8)
        + "/" + SEGMENTS_ENDPOINT + URLEncoder.encode(segmentName, UTF_8) + "/" + METADATA_ENDPOINT);
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      String metadataResponse = IOUtils.toString(content);
      metadata = getMetadataFromResponse(metadataResponse);
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return metadata;
  }

  private List<String> getSegmentsFromResponse(String response) {
    String[] allSegments = response.replaceAll("\\[|\\]|\"", "").split(",");
    return Arrays.asList(allSegments);
  }

  private Map<String, String> getMetadataFromResponse(String response) {
    Map<String, String> metadata = new HashMap<>();
    String cleanUpResponse = response.replaceAll("\\[|\\]|\"|\\{|\\}|\\\\", "");
    String[] allProperties = cleanUpResponse.replace("state:", "").split(",");
    for (String property : allProperties) {
      String[] tokens = property.split(":", 2);
      metadata.put(tokens[0], tokens[1]);
    }
    return metadata;
  }

}
