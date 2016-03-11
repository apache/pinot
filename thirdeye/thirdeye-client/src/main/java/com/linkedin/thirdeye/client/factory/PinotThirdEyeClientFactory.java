package com.linkedin.thirdeye.client.factory;

import static com.linkedin.thirdeye.client.PinotThirdEyeClient.BROKERS_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CLUSTER_NAME_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_HOST_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_PORT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.FIXED_COLLECTIONS_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.TAG_PROPERTY_KEY;

import java.util.Arrays;
import java.util.List;
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
    assertContainsKeys(props, CONTROLLER_HOST_PROPERTY_KEY, CONTROLLER_PORT_PROPERTY_KEY);
    PinotThirdEyeClient client;
    String controllerHost = props.getProperty(CONTROLLER_HOST_PROPERTY_KEY);
    int controllerPort = Integer.parseInt(props.getProperty(CONTROLLER_PORT_PROPERTY_KEY));
    if (props.containsKey(BROKERS_PROPERTY_KEY)) {
      client = PinotThirdEyeClient.fromHostList(controllerHost, controllerPort,
          props.getProperty(BROKERS_PROPERTY_KEY).split(","));
    } else {
      assertContainsKeys(props, ZK_URL_PROPERTY_KEY, CLUSTER_NAME_PROPERTY_KEY, TAG_PROPERTY_KEY);
      String zkUrl = props.getProperty(ZK_URL_PROPERTY_KEY);
      String clusterName = props.getProperty(CLUSTER_NAME_PROPERTY_KEY);
      String tag = props.getProperty(TAG_PROPERTY_KEY);
      client = PinotThirdEyeClient.fromZookeeper(getConfig(), controllerHost, controllerPort, zkUrl,
          clusterName, tag);
    }

    if (props.containsKey(FIXED_COLLECTIONS_PROPERTY_KEY)) {
      String collectionString = props.getProperty(FIXED_COLLECTIONS_PROPERTY_KEY);
      List<String> collections = Arrays.asList(collectionString.split(","));
      client.setFixedCollections(collections);
    }
    return client;
  }

}
