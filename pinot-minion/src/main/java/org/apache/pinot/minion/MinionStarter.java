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
package org.apache.pinot.minion;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * The class <code>MinionStarter</code> provides methods to start and stop the Pinot Minion.
 * <p>Pinot Minion will automatically join the given Helix cluster as a participant.
 */
public class MinionStarter extends BaseMinionStarter {

  public MinionStarter() {
  }

  @Deprecated
  public MinionStarter(String clusterName, String zkServers, PinotConfiguration minionConfig)
      throws Exception {
    init(applyMinionConfigs(minionConfig, clusterName, zkServers));
  }

  @Deprecated
  private static PinotConfiguration applyMinionConfigs(PinotConfiguration minionConfig, String clusterName,
      String zkServers) {
    minionConfig.setProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    minionConfig.setProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkServers);
    return minionConfig;
  }

  @Deprecated
  public MinionStarter(PinotConfiguration config)
      throws Exception {
    init(config);
  }
}
