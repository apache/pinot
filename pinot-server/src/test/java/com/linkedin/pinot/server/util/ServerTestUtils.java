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
