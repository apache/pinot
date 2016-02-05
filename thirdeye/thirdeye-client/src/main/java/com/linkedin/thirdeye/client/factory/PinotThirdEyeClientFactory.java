package com.linkedin.thirdeye.client.factory;

import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_HOST_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_PORT_PROPERTY_KEY;

import java.util.Properties;

import com.linkedin.thirdeye.client.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * See
 * {@link PinotThirdEyeClient#fromZookeeper(com.linkedin.thirdeye.client.CachedThirdEyeClientConfig, String, int, String)}
 * @author jteoh
 */
public class PinotThirdEyeClientFactory extends BaseThirdEyeClientFactory {
  public static final String ZK_URL_PROPERTY_KEY = "zkUrl";

  @Override
  public ThirdEyeClient getRawClient(Properties props) {
    if (!props.containsKey(CONTROLLER_HOST_PROPERTY_KEY)
        || !props.containsKey(CONTROLLER_PORT_PROPERTY_KEY)
        || !props.containsKey(ZK_URL_PROPERTY_KEY)) {
      throw new IllegalArgumentException(
          "Config file must contain mappings for " + CONTROLLER_HOST_PROPERTY_KEY + ", "
              + CONTROLLER_PORT_PROPERTY_KEY + ", and " + ZK_URL_PROPERTY_KEY + ": " + props);
    }
    String controllerHost = props.getProperty(CONTROLLER_HOST_PROPERTY_KEY);
    int controllerPort = Integer.parseInt(props.getProperty(CONTROLLER_PORT_PROPERTY_KEY));
    String zkUrl = props.getProperty(ZK_URL_PROPERTY_KEY);
    return PinotThirdEyeClient.fromZookeeper(getConfig(), controllerHost, controllerPort, zkUrl);
  }

}
