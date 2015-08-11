package com.linkedin.thirdeye.anomaly.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.net.HostAndPort;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.DefaultThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 * Facilitates lookup of star-tree configuration and shared third-eye clients by collection name.
 */
public class ThirdEyeMultiClient {

  private static final int DEFAULT_CACHE_EXPIRATION_MINUTES = 30;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeMultiClient.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<String, StarTreeConfig> starTreeCache = new HashMap<>();
  private final Map<String, ThirdEyeClient> thirdEyeClients = new HashMap<>();
  private final Map<String, HostAndPort> collectionToHostAndPort = new HashMap<>();

  public ThirdEyeMultiClient(List<AnomalyDetectionDriverConfig> collectionConfigs) {
    DefaultThirdEyeClientConfig thirdEyeClientConfig = new DefaultThirdEyeClientConfig();
    thirdEyeClientConfig.setExpirationTime(DEFAULT_CACHE_EXPIRATION_MINUTES);
    thirdEyeClientConfig.setExpirationUnit(TimeUnit.MINUTES);
    thirdEyeClientConfig.setExpireAfterAccess(false);

    for (AnomalyDetectionDriverConfig collectionConfig : collectionConfigs)
    {
      collectionToHostAndPort.put(collectionConfig.getCollectionName(),
          HostAndPort.fromParts(collectionConfig.getThirdEyeServerHost(), collectionConfig.getThirdEyeServerPort()));
      thirdEyeClients.put(collectionConfig.getCollectionName(),
          new DefaultThirdEyeClient(collectionConfig.getThirdEyeServerHost(), collectionConfig.getThirdEyeServerPort(),
              thirdEyeClientConfig));
    }
  }

  /**
   * @param driverConfig
   * @return
   *  The starTreeConfig corresponding to the collection
   */
  public synchronized StarTreeConfig getStarTreeConfig(String collection) throws IOException {
    String urlString = "http://" + collectionToHostAndPort.get(collection).toString()
        + "/collections/" + collection;
    LOGGER.info("getting star-tree : {}", urlString);

    StarTreeConfig cached = starTreeCache.get(urlString);
    if (cached != null) {
      LOGGER.info("cache hit for {}", urlString);
      return cached;
    }

    URL url = new URL(urlString);
    StarTreeConfig result = OBJECT_MAPPER.readValue(new InputStreamReader(url.openStream(), "UTF-8"),
        StarTreeConfig.class);

    // cache the result
    starTreeCache.put(urlString, result);

    return result;
  }

  /**
   * @param collection
   * @return
   *  The third eye client corresponding to the collection name
   */
  public ThirdEyeClient getClient(String collection) {
    return thirdEyeClients.get(collection);
  }

  /**
   * Close all the clients
   * @throws Exception
   */
  public void close() throws Exception {
    for (ThirdEyeClient thirdEyeClient : thirdEyeClients.values()) {
      thirdEyeClient.close();
    }
  }

}
