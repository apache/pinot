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

import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;

/**
 * Class to implement StartController command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class StartControllerCommand implements Command {
  @Option(name="-cfgFile", required=true, metaVar="<fileName>")
  String _cfgFile = null;

  @Option(name="-clusterName", required=true, metaVar="<name of the cluster>")
  String _clusterName = null;

  @Option(name="-tableName", required=false, metaVar="<name of the cluster>")
  String _tableName = null;

  @Option(name="-controllerPort", required=true, metaVar="<data directory>")
  String _controllerPort = null;

  @Option(name="-dataDir", required=false, metaVar="<data directory>")
  String _dataDir = null;

  @Option(name="-zkAddress", required=true, metaVar="<Zookeeper URL to connect to>")
  String _zkAddress = null;

  public void init(String cfgFile, String clusterName, String tableName,
        String controllerPort, String dataDir, String zkAddress) {
    _cfgFile = cfgFile;
    _clusterName = clusterName;

    _tableName = tableName;
    _controllerPort = controllerPort;
    _zkAddress = zkAddress;
  }

  @Override
  public String toString() {
    return ("StartController " + _cfgFile + " " + _clusterName + " " + _tableName + " " +
            _zkAddress + " " + _controllerPort + " " + _dataDir);
  }

  @Override
  public boolean execute() throws Exception {
    final ControllerConf conf = new ControllerConf();

    conf.setControllerHost("localhost");
    conf.setControllerPort(_controllerPort);
    conf.setDataDir(_dataDir);
    conf.setZkStr(_zkAddress);

    conf.setHelixClusterName(_clusterName);
    conf.setControllerVipHost("localhost");

    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setValidationControllerFrequencyInSeconds(3600);

    final ControllerStarter starter = new ControllerStarter(conf);
    System.out.println(conf.getQueryConsole());

    starter.start();

    // Resource creation is optional, otherwise there is a cyclic-dependency between
    // starting server and controller. One time call without these two arguments required to
    // break the cycle.
    if ((_dataDir == null) || (_tableName == null)) {
      return true;
    }

    // Create DataResource
    int numInstances = 1;
    int numReplicas = 1;
    String segmentAssignmentStrategy = "BalanceNumSegmentAssignmentStrategy";

    // Create a DataResource
    DataResource dataResource =
        createDataResource(numInstances, numReplicas, _clusterName, segmentAssignmentStrategy);

    starter.getHelixResourceManager().handleCreateNewDataResource(dataResource);
    starter.getHelixResourceManager().handleAddTableToDataResource(dataResource);

    return true;
  }

  public DataResource createDataResource(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE,
        CommonConstants.Helix.DataSourceRequestType.CREATE);

    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, Helix.ResourceType.OFFLINE.toString());
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, _tableName);
    props.put(CommonConstants.Helix.DataSource.TIME_COLUMN_NAME, "daysSinceEpoch");
    props.put(CommonConstants.Helix.DataSource.TIME_TYPE, "DAYS");
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES,
        String.valueOf(numInstances));

    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT, "DAYS");
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE, "300");
    props.put(CommonConstants.Helix.DataSource.PUSH_FREQUENCY, "daily");
    props.put(CommonConstants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY,
        segmentAssignmentStrategy);

    props.put(CommonConstants.Helix.DataSource.BROKER_TAG_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, "1");

    final DataResource res = DataResource.fromMap(props);
    return res;
  }
}
