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
package org.apache.pinot.broker.broker.helix;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.NoArgsConstructor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


/**
 * Startable implementation for Pinot broker.
 * Contains methods to start and stop a Pinot broker
 */
@NoArgsConstructor
@SuppressWarnings("unused")
public class HelixBrokerStarter extends BaseBrokerStarter {
  @Deprecated
  public HelixBrokerStarter(PinotConfiguration brokerConf, String clusterName, String zkServer)
      throws Exception {
    init(applyBrokerConfigs(brokerConf, clusterName, zkServer, null));
  }

  @Deprecated
  public HelixBrokerStarter(PinotConfiguration brokerConf, String clusterName, String zkServer,
      @Nullable String brokerHost)
      throws Exception {
    init(applyBrokerConfigs(brokerConf, clusterName, zkServer, brokerHost));
  }

  @Deprecated
  private static PinotConfiguration applyBrokerConfigs(PinotConfiguration brokerConf, String clusterName,
      String zkServers, @Nullable String brokerHost) {
    brokerConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    brokerConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkServers);
    if (brokerHost == null) {
      brokerConf.clearProperty(Broker.CONFIG_OF_BROKER_HOSTNAME);
    } else {
      brokerConf.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, brokerHost);
    }
    return brokerConf;
  }

  @Deprecated
  public HelixBrokerStarter(PinotConfiguration brokerConf)
      throws Exception {
    init(brokerConf);
  }

  public static HelixBrokerStarter getDefault()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();

    properties.put(Helix.KEY_OF_BROKER_QUERY_PORT, 5001);
    properties.put(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    properties.put(Helix.CONFIG_OF_CLUSTER_NAME, "quickstart");
    properties.put(Helix.CONFIG_OF_ZOOKEEPR_SERVER, "localhost:2122");
    HelixBrokerStarter helixBrokerStarter = new HelixBrokerStarter();
    helixBrokerStarter.init(new PinotConfiguration(properties));
    return helixBrokerStarter;
  }

  public static void main(String[] args)
      throws Exception {
    getDefault().start();
  }
}
