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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;

/**
 * Class to implement StartServer command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class StartServerCommand implements Command {
  @Option(name="-cfgFile", required=true, metaVar="<fileName>")
  String _cfgFile = null;

  @Option(name="-clusterName", required=true, metaVar="<name of the cluster>")
  String _clusterName = null;

  @Option(name="-zkAddress", required=true, metaVar="<Zookeeper URL to connect to>")
  String _zkAddress = null;

  @Option(name="-dataDir", required=true, metaVar="<instance data directory>")
  String _dataDir = null;

  @Option(name="-segmentDir", required=true, metaVar="<segment tar directory>")
  String _segmentDir = null;

  //DEFAULT_SERVER_NETTY_PORT
  @Option(name="-port", required=false, metaVar="<server netty port>")
  int _port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  public void init(String cfgFile, String clusterName, String zkAddress, String dataDir, String segmentDir) {
    _cfgFile = cfgFile;
    _clusterName = clusterName;

    _zkAddress = zkAddress;
    _dataDir = dataDir;
    _segmentDir = segmentDir;
  }

  @Override
  public String toString() {
    return ("StartServer " + _cfgFile + " " + _clusterName + " " + _zkAddress + " " + _dataDir + " " + _segmentDir);
  }

  @Override
  public boolean execute() throws Exception {
    final Configuration configuration = new PropertiesConfiguration();

    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, _port);
    configuration.addProperty("pinot.server.instance.dataDir", _dataDir + _port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", _segmentDir + _port + "/segmentTar");

    final HelixServerStarter pinotHelixStarter =
        new HelixServerStarter(_clusterName, _zkAddress, configuration);

    return true;
  }
}
