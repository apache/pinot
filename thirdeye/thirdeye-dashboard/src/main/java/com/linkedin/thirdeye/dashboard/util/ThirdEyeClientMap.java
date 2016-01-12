package com.linkedin.thirdeye.dashboard.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.factory.DefaultThirdEyeClientFactory;
import com.linkedin.thirdeye.client.factory.ThirdEyeClientFactory;

/**
 * Map to create and cache {@link com.linkedin.thirdeye.client.ThirdEyeClient} instances for each
 * provided serverUri. It provides host and port to a {@link ThirdEyeClientFactory} instance to
 * create clients.
 * @author jteoh
 */
public class ThirdEyeClientMap {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeClientMap.class);

  private final Map<String, ThirdEyeClient> clientMap = new HashMap<>();
  private final ThirdEyeClientFactory factory;
  private static final Pattern HOST_PORT_REGEX = Pattern.compile("(http://)?([^:]+):([0-9]+)");

  public ThirdEyeClientMap() {
    this(new DefaultThirdEyeClientFactory());
  }

  // TODO: Expose cache expiration policy via config similar to DefaultThirdEyeClientConfig
  public ThirdEyeClientMap(ThirdEyeClientFactory factory) {
    LOGGER.info("Initialized with factory: {}", factory);
    this.factory = factory;
  }

  public ThirdEyeClient get(String serverUri) {
    if (!clientMap.containsKey(serverUri)) {
      LOGGER.info("Creating ThirdEyeClient for {}", serverUri);
      ThirdEyeClient client;
      Matcher matcher = HOST_PORT_REGEX.matcher(serverUri);
      if (matcher.matches()) {
        String hostname = matcher.group(2);
        String port = matcher.group(3);
        Properties config = new Properties();
        config.setProperty(DefaultThirdEyeClientFactory.HOST, hostname);
        config.setProperty(DefaultThirdEyeClientFactory.PORT, port);
        client = factory.getClient(config);
      } else {
        throw new IllegalArgumentException("Server uri does not contain host and port number");
      }

      clientMap.put(serverUri, client);
    }
    return clientMap.get(serverUri);
  }

  public ThirdEyeClient set(String serverUri, ThirdEyeClient client) {
    return clientMap.put(serverUri, client);
  }

  public void clear() throws Exception {
    LOGGER.info("Clearing ThirdEyeClient entries...");
    for (ThirdEyeClient thirdEyeClient : clientMap.values()) {
      thirdEyeClient.clear();
    }
    clientMap.clear();
  }
}
