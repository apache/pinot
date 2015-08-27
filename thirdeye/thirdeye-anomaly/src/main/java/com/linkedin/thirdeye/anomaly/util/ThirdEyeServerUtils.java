package com.linkedin.thirdeye.anomaly.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 *
 */
public class ThirdEyeServerUtils {

  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * @param host
   * @param port
   * @param collection
   * @return
   *  The StartTreeConfig for the collection.
   * @throws IOException
   */
  public static StarTreeConfig getStarTreeConfig(String host, short port, String collection) throws IOException {
    String urlString = "http://" + host + ":" + port + "/collections/" + collection;
    URL url = new URL(urlString);
    return OBJECT_MAPPER.readValue(new InputStreamReader(url.openStream(), "UTF-8"), StarTreeConfig.class);
  }

  /**
   * @param host
   * @param port
   * @param collection
   * @return
   *  The latest segment available at the ThirdEye server.
   * @throws IOException
   */
  public static long getLatestTime(String host, short port, String collection) throws IOException {
    String urlString = "http://" + host + ":" + port + "/collections/" + collection + "/segments";
    URL url = new URL(urlString);
    final CollectionType listType = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class,
        SegmentDescriptor.class);
    List<SegmentDescriptor> segments = OBJECT_MAPPER.readValue(new InputStreamReader(url.openStream(), "UTF-8"),
        listType);
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getEndDataTime() != null && (latestDataTime == null ||
          segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
        latestDataTime = segment.getEndDataTime();
      }
    }
    return latestDataTime.getMillis();
  }
}
