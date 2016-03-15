package com.linkedin.thirdeye.bootstrap.segment.push;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class SegmentPushControllerAPIs {

  private static Logger LOGGER = LoggerFactory.getLogger(SegmentPushControllerAPIs.class);
  private String[] controllerHosts;
  private int controllerPort;
  private HttpHost controllerHttpHost;

  private static String DAILY_SCHEDULE = "DAILY";
  private static String HOURLY_SCHEDULE = "HOURLY";
  private static String SEGMENTS_ENDPOINT = "segments/";
  private static String TABLES_ENDPOINT = "tables/";
  private static String DROP_PARAMETERS = "?state=drop";
  private static String UTF_8 = "UTF-8";
  private static String SEGMENT_JOINER = "_";
  private static long TIMEOUT = 120000;
  private static String SUCCESS = "success";

  SegmentPushControllerAPIs(String[] controllerHosts, String controllerPort) {
    this.controllerHosts = controllerHosts;
    this.controllerPort = Integer.valueOf(controllerPort);
  }

  public void deleteOverlappingSegments(String tableName, String segmentName) throws IOException {
    if (segmentName.contains(DAILY_SCHEDULE)) {
      for (String controllerHost : controllerHosts) {
        controllerHttpHost = new HttpHost(controllerHost, controllerPort);

        LOGGER.info("Getting overlapped segments*************");
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

      for (String segment : allSegments) {
        if (segment.startsWith(pattern)) {
          LOGGER.info("Found overlapping segment {}", segment);
          overlappingSegments.add(segment);
        }
      }
    }
    return overlappingSegments;
  }

  private String getOverlapPattern(String segmentName, String tablename) {
    String pattern = null;
    String[] tokens = segmentName.split(SEGMENT_JOINER);
    if (tokens.length > 2 && tokens[2].lastIndexOf("-") != -1) {
      String datePrefix = tokens[2].substring(0, tokens[2].lastIndexOf("-"));
      pattern = Joiner.on(SEGMENT_JOINER).join(tablename, HOURLY_SCHEDULE, datePrefix);
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

  private List<String> getSegmentsFromResponse(String response) {
    String[] allSegments = response.replaceAll("\\[|\\]|\"", "").split(",");
    return Arrays.asList(allSegments);
  }

  private void deleteOverlappingSegments(String tablename, List<String> overlappingSegments) throws IOException {

    for (String segment : overlappingSegments) {
      String response = "";
      long elapsedTime = 0;
      long startTimeMillis = System.currentTimeMillis();
      while (elapsedTime < TIMEOUT && !response.toLowerCase().contains(SUCCESS)) {
        response = deleteSegment(tablename, segment);
        LOGGER.info("Response {} while deleting segment {} from table {}", response, segment, tablename);
        long currentTimeMillis = System.currentTimeMillis();
        elapsedTime = elapsedTime + (currentTimeMillis - startTimeMillis);
      }
    }
  }

  private String deleteSegment(String tablename, String segmentName) throws IOException {
    String response = null;

    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet(TABLES_ENDPOINT + URLEncoder.encode(tablename, UTF_8)
        + "/" + SEGMENTS_ENDPOINT + URLEncoder.encode(segmentName, UTF_8)
        + DROP_PARAMETERS);
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      response = IOUtils.toString(content);

    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }

    return response;
  }

}
