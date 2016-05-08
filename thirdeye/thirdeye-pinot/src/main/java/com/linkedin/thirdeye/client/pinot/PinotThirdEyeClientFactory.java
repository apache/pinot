package com.linkedin.thirdeye.client.pinot;

import static com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient.CLUSTER_NAME_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient.CONTROLLER_HOST_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient.CONTROLLER_PORT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient.FIXED_COLLECTIONS_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient.TAG_PROPERTY_KEY;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.detector.ThirdEyeDetectorConfiguration;

/**
 * See
 * {@link PinotThirdEyeClient#fromZookeeper(com.linkedin.thirdeye.client.CachedThirdEyeClientConfig, String, int, String)}
 * @author jteoh
 */
public class PinotThirdEyeClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeClient.class);

  public static final String ZK_URL_PROPERTY_KEY = "zkUrl";

  public ThirdEyeClient getRawClient(Properties props) {
    if (!props.containsKey(CONTROLLER_HOST_PROPERTY_KEY)
        || !props.containsKey(CONTROLLER_PORT_PROPERTY_KEY)
        || !props.containsKey(ZK_URL_PROPERTY_KEY) || !props.containsKey(CLUSTER_NAME_PROPERTY_KEY)
        || !props.containsKey(TAG_PROPERTY_KEY)) {
      throw new IllegalArgumentException(
          "Config file must contain mappings for " + CONTROLLER_HOST_PROPERTY_KEY + ", "
              + CONTROLLER_PORT_PROPERTY_KEY + ", " + ZK_URL_PROPERTY_KEY + ", "
              + CLUSTER_NAME_PROPERTY_KEY + " and " + TAG_PROPERTY_KEY + " : " + props);
    }
    String controllerHost = props.getProperty(CONTROLLER_HOST_PROPERTY_KEY);
    int controllerPort = Integer.parseInt(props.getProperty(CONTROLLER_PORT_PROPERTY_KEY));
    String zkUrl = props.getProperty(ZK_URL_PROPERTY_KEY);
    String clusterName = props.getProperty(CLUSTER_NAME_PROPERTY_KEY);
    String tag = props.getProperty(TAG_PROPERTY_KEY);
    PinotThirdEyeClient client =
        PinotThirdEyeClient.fromZookeeper(controllerHost, controllerPort, zkUrl, clusterName, tag);
    if (props.containsKey(FIXED_COLLECTIONS_PROPERTY_KEY)) {
      String collectionString = props.getProperty(FIXED_COLLECTIONS_PROPERTY_KEY);
      List<String> collections = Arrays.asList(collectionString.split(","));
      client.setFixedCollections(collections);
    }
    return client;
  }

  public static ThirdEyeClient createThirdEyeClient(ThirdEyeConfiguration config) throws Exception {
    File clientConfigDir = new File(config.getRootDir(), "client-config");
    File clientConfigFile = new File(clientConfigDir, config.getClient() + ".yml");
    PinotThirdEyeClientConfig thirdEyeClientConfig =
        PinotThirdEyeClientConfig.fromFile(clientConfigFile);
    LOG.info("Loaded client config:{}", thirdEyeClientConfig);
    ThirdEyeClient thirdEyeClient = PinotThirdEyeClient.fromClientConfig(thirdEyeClientConfig);
    return thirdEyeClient;
  }

}
