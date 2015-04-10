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
package com.linkedin.pinot.tools.admin.command;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.utils.CommonConstants;

/**
 * Class to implement StartBroker command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class StartBrokerCommand implements Command {
  @Option(name="-cfgFile", required=true, metaVar="<fileName>")
  String _cfgFile = null;

  @Option(name="-clusterName", required=true, metaVar="<name of the cluster>")
  String _clusterName = null;

  @Option(name="-zkAddress", required=true, metaVar="<Zookeeper address to connect to>")
  String _zkAddress = null;

  @Option(name="-brokerInstName", required=false, metaVar="<Broker instance name>")
  String _brokerInstName = "Broker_localhost_";

  @Option(name="-brokerHostName", required=false, metaVar="<Broker host name>")
  String _brokerHostName = "localhost";

  @Option(name="-queryPort", required=false, metaVar="<Query port number>")
  int _queryPort = CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;;

  @Override
  public String toString() {
    return ("StartBroker " + _cfgFile + " " + _clusterName + " " + _zkAddress);
  }

  public void init(String cfgFile, String clusterName, String zkAddress) {
    _cfgFile = cfgFile;
    _clusterName = clusterName;
    _zkAddress = zkAddress;
  }

  @Override
  public boolean execute() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    String brokerInstanceName = _brokerInstName + "_" + _brokerHostName + "_" + _queryPort;

    configuration.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, _queryPort);
    configuration.setProperty("instanceId", brokerInstanceName);

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter(_clusterName, _zkAddress, configuration);

    return true;
  }
}
