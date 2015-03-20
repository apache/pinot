package com.linkedin.pinot.server.util;

import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import org.apache.commons.configuration.Configuration;


/**
 * Utilities to start servers during unit tests.
 *
 * @author jfim
 */
public class ServerTestUtils {
  static class PublicHelixServerStarter extends HelixServerStarter {
    public PublicHelixServerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
        throws Exception {
      super(helixClusterName, zkServer, pinotHelixProperties);
    }

    public void stop() {
      _helixManager.disconnect();
    }
  }

  public static Configuration getDefaultServerConfiguration() {
    return DefaultHelixStarterServerConfig.loadDefaultServerConf();
  }

  public static PublicHelixServerStarter startServer(final String clusterName, final String zkStr, final Configuration configuration) {
    try {
      return new PublicHelixServerStarter(clusterName, zkStr, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void stopServer(final PublicHelixServerStarter serverStarter) {
    serverStarter.stop();
  }
}
