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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartMinion command.
 *
 */
public class StartMinionCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartMinionCommand.class);
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;
  @Option(name = "-minionHost", required = false, metaVar = "<String>", usage = "Host name for minion.")
  private String _minionHost;
  @Option(name = "-minionPort", required = false, metaVar = "<int>", usage = "Port number to start the minion at.")
  private int _minionPort = CommonConstants.Minion.DEFAULT_HELIX_PORT;
  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;
  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";
  @Option(name = "-configFileName", required = false, metaVar = "<Config File Name>", usage = "Minion Starter Config file.", forbids = {"-minionHost", "-minionPort"})
  private String _configFileName;
  private MinionStarter _minionStarter;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StartMinion";
  }

  @Override
  public String toString() {
    if (_configFileName != null) {
      return ("StartMinion -zkAddress " + _zkAddress + " -configFileName " + _configFileName);
    } else {
      return ("StartMinion -minionHost " + _minionHost + " -minionPort " + _minionPort + " -zkAddress " + _zkAddress);
    }
  }

  @Override
  public void cleanup() {
    if (_minionStarter != null) {
      _minionStarter.stop();
    }
  }

  @Override
  public String description() {
    return "Start the Pinot Minion process at the specified port";
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.MINION, getMinionConf());
      startServiceManagerCommand.execute();
      String pidFile = ".pinotAdminMinion-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting minion, exiting", e);
      System.exit(-1);
      return false;
    }
  }

  private Map<String, Object> getMinionConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    if (_configFileName != null) {
      return PinotConfigUtils.readConfigFromFile(_configFileName);
    }
    return PinotConfigUtils.generateMinionConf(_minionHost, _minionPort);
  }
}
