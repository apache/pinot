/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.push;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;

/**
 * Contains APIs which can be used for segment operations
 * such as listing, deleting overlap
 */
public class SegmentPushControllerAPIs {

  private static Logger LOGGER = LoggerFactory.getLogger(SegmentPushControllerAPIs.class);
  private String[] controllerHosts;
  private int controllerPort;
  private HttpHost controllerHttpHost;

  private static final String OFFLINE_SEGMENTS = "OFFLINE";
  private static String DAILY_SCHEDULE = "DAILY";
  private static String HOURLY_SCHEDULE = "HOURLY";
  private static String SEGMENTS_ENDPOINT = "/segments/";
  private static String TABLES_ENDPOINT = "/tables/";
  private static String TYPE_PARAMETER = "?type=offline";
  private static String UTF_8 = "UTF-8";
  private static long TIMEOUT = 120000;
  private static String DATE_JOINER = "-";

  SegmentPushControllerAPIs(String[] controllerHosts, String controllerPort) {
    this.controllerHosts = controllerHosts;
    this.controllerPort = Integer.valueOf(controllerPort);
  }

  public void deleteOverlappingSegments(String tableName, String segmentName) throws IOException {
    if (segmentName.contains(DAILY_SCHEDULE)) {
      for (String controllerHost : controllerHosts) {
        controllerHttpHost = new HttpHost(controllerHost, controllerPort);

        LOGGER.info("Getting overlapped segments for {}*************", segmentName);
        List<String> overlappingSegments = getOverlappingSegments(tableName, segmentName);

        if (overlappingSegments.isEmpty()) {
          LOGGER.info("No overlapping segments found");
        } else {
          LOGGER.info("Deleting overlapped segments****************");
          deleteOverlappingSegments(tableName, overlappingSegments);
        }
      }
    } else {
      LOGGER.info("No overlapping segments to delete for HOURLY");
    }
  }

  private List<String> getOverlappingSegments(String tablename, String segmentName) throws IOException {

    List<String> overlappingSegments = new ArrayList<>();
    String pattern = getOverlapPattern(segmentName, tablename);
    if (pattern != null) {
      LOGGER.info("Finding segments overlapping to {} with pattern {}", segmentName, pattern);
      List<String> allSegments = getAllSegments(tablename, segmentName);
      overlappingSegments = getOverlappingSegments(allSegments, pattern);
    }
    return overlappingSegments;
  }

  public List<String> getOverlappingSegments(List<String> allSegments, String pattern) {
    List<String> overlappingSegments = new ArrayList<>();
    for (String segment : allSegments) {
      if (segment.startsWith(pattern)) {
        LOGGER.info("Found overlapping segment {}", segment);
        overlappingSegments.add(segment);
      }
    }
    return overlappingSegments;
  }

  public String getOverlapPattern(String segmentName, String tablename) {
    String pattern = null;
    // segment name format: table[_*]Name_schedule_startDate_endDate
    String[] tokens = segmentName.split(ThirdEyeConstants.SEGMENT_JOINER);
    int size = tokens.length;
    if (size > 3) {
      String startDateToken = tokens[size - 2];
      if (startDateToken.lastIndexOf(DATE_JOINER) != -1) {
        String datePrefix = startDateToken.substring(0, startDateToken.lastIndexOf(DATE_JOINER));
        pattern = Joiner.on(ThirdEyeConstants.SEGMENT_JOINER).join(tablename, HOURLY_SCHEDULE, datePrefix);
      }
    }
    return pattern;
  }

  private List<String> getAllSegments(String tablename, String segmentName) throws IOException {
    List<String> allSegments = new ArrayList<>();

    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet(SEGMENTS_ENDPOINT + URLEncoder.encode(tablename, UTF_8));
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      JsonNode segmentsData = new ObjectMapper().readTree(content);

      if (segmentsData != null) {
        JsonNode offlineSegments = segmentsData.get(0).get(OFFLINE_SEGMENTS);
        if (offlineSegments != null) {
          for (JsonNode segment : offlineSegments) {
            allSegments.add(segment.asText());
          }
        }
      }
      LOGGER.info("All segments : {}", allSegments);
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return allSegments;
  }

  private boolean isDeleteSuccessful(String tablename, String segmentName) throws IOException {

    boolean deleteSuccessful = false;
    HttpClient controllerClient = new DefaultHttpClient();
    // this endpoint gets from ideal state
    HttpGet req = new HttpGet(TABLES_ENDPOINT + URLEncoder.encode(tablename, UTF_8) + SEGMENTS_ENDPOINT);
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      String response = IOUtils.toString(content);
      LOGGER.info("All segments from ideal state {}", response);
      String decoratedSegmentName = "\\\""+segmentName+"\\\"";
      LOGGER.info("Decorated segment name {}", decoratedSegmentName);
      if (!response.contains(decoratedSegmentName)) {
        deleteSuccessful = true;
        LOGGER.info("Delete successful");
      } else {
        LOGGER.info("Delete failed");
      }
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }

    }
    return deleteSuccessful;

  }


  private void deleteOverlappingSegments(String tablename, List<String> overlappingSegments) throws IOException {

    for (String segment : overlappingSegments) {
      boolean deleteSuccessful = false;
      long elapsedTime = 0;
      long startTimeMillis = System.currentTimeMillis();
      while (elapsedTime < TIMEOUT && !deleteSuccessful) {
        deleteSuccessful = deleteSegment(tablename, segment);
        LOGGER.info("Response {} while deleting segment {} from table {}", deleteSuccessful, segment, tablename);
        long currentTimeMillis = System.currentTimeMillis();
        elapsedTime = elapsedTime + (currentTimeMillis - startTimeMillis);
      }
    }
  }

  private boolean deleteSegment(String tablename, String segmentName) throws IOException {
    boolean deleteSuccessful = false;

    HttpClient controllerClient = new DefaultHttpClient();
    HttpDelete req = new HttpDelete(SEGMENTS_ENDPOINT + URLEncoder.encode(tablename, UTF_8) + "/"
        + URLEncoder.encode(segmentName, UTF_8)
        + TYPE_PARAMETER);
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res == null || res.getStatusLine() == null || res.getStatusLine().getStatusCode() != 200
          || !isDeleteSuccessful(tablename, segmentName)) {
        LOGGER.info("Exception in deleting segment, trying again {}", res);
      } else {
        deleteSuccessful = true;
      }
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return deleteSuccessful;
  }

}
