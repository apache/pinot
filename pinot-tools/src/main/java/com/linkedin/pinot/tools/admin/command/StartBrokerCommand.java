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

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.utils.CommonConstants;

/**
 * Class to implement StartBroker command.
 *
 */
public class StartBrokerCommand extends AbstractBaseCommand implements Command {
  @Option(name="-clusterName", required=true, metaVar="<string>", usage="Name of the cluster.")
  private String _clusterName = null;

  @Option(name="-zkAddress", required=true, metaVar="<http>", usage="Zookeeper http address.")
  private String _zkAddress = null;

  @Option(name="-brokerInstName", required=false, metaVar="<string>", usage="Instance name of the broker.")
  private String _brokerInstName = "Broker_localhost_";

  @Option(name="-brokerHostName", required=false, metaVar="<string>", usage="Host name where broker is running.")
  private String _brokerHostName = "localhost";

  @Option(name="-queryPort", required=false, metaVar="<int>", usage="Broker port number to use for query.")
  private int _queryPort = CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;;

  @Option(name="-help", required=false, help=true, usage="Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StartBroker";
  }

  @Override
  public String toString() {
    return ("StartBrokerCommand -brokerInstName " + _brokerInstName + " -brokerHostName " + _brokerHostName +
        " -queryPort " + _queryPort);
  }

  @Override
  public void cleanup() {

  }

  public StartBrokerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public StartBrokerCommand setPort(int port) {
    _queryPort = port;
    return this;
  }

  public StartBrokerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    String brokerInstanceName = _brokerInstName + "_" + _brokerHostName + "_" + _queryPort;

    configuration.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, _queryPort);
    configuration.setProperty("instanceId", brokerInstanceName);

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter(_clusterName, _zkAddress, configuration);

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".pinotAdminBroker.pid");
    return true;
  }
}
