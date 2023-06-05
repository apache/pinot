/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.starter.helix;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;


/**
 * Starter for Pinot server.
 * <p>When the server starts for the first time, it will automatically join the Helix cluster with the default tag.
 * <ul>
 *   <li>
 *     Optional start-up checks:
 *     <ul>
 *       <li>Service status check (ON by default)</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Optional shut-down checks:
 *     <ul>
 *       <li>Query check (drains and finishes existing queries, ON by default)</li>
 *       <li>Resource check (wait for all resources OFFLINE, OFF by default)</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class HelixServerStarter extends BaseServerStarter {

  public HelixServerStarter() {
  }

  @Deprecated
  public HelixServerStarter(String helixClusterName, String zkAddress, PinotConfiguration serverConf)
      throws Exception {
    init(applyServerConfig(serverConf, helixClusterName, zkAddress));
  }

  @Deprecated
  private static PinotConfiguration applyServerConfig(PinotConfiguration serverConf, String helixClusterName,
      String zkAddress) {
    serverConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, helixClusterName);
    serverConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkAddress);
    return serverConf;
  }

  @Deprecated
  public HelixServerStarter(PinotConfiguration serverConf)
      throws Exception {
    init(serverConf);
  }

  /**
   * This method is for reference purpose only.
   */
  public static HelixServerStarter startDefault()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    int port = 8003;
    properties.put(Helix.CONFIG_OF_CLUSTER_NAME, "quickstart");
    properties.put(Helix.CONFIG_OF_ZOOKEEPR_SERVER, "localhost:2191");
    properties.put(Helix.KEY_OF_SERVER_NETTY_PORT, port);
    properties.put(Server.CONFIG_OF_INSTANCE_DATA_DIR, "/tmp/PinotServer/test" + port + "/index");
    properties.put(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, "/tmp/PinotServer/test" + port + "/segmentTar");

    HelixServerStarter serverStarter = new HelixServerStarter();
    serverStarter.init(new PinotConfiguration(properties));
    serverStarter.start();
    return serverStarter;
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
