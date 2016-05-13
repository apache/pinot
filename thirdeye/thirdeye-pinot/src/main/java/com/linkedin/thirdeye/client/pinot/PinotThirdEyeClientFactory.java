package com.linkedin.thirdeye.client.pinot;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

/**
 * See
 * {@link PinotThirdEyeClient#fromZookeeper(com.linkedin.thirdeye.client.CachedThirdEyeClientConfig, String, int, String)}
 * @author jteoh
 */
public class PinotThirdEyeClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeClient.class);

  public static final String ZK_URL_PROPERTY_KEY = "zkUrl";

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
