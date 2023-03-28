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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordStreamingSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import picocli.CommandLine;


@CommandLine.Command(name = "ShowClusterInfo")
public class ShowClusterInfoCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowClusterInfoCommand.class.getName());

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster clusterName.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @CommandLine.Option(names = {"-tables"}, required = false, description = "Comma separated table names.")
  private String _tables = "";

  @CommandLine.Option(names = {"-tags"}, required = false, description = "Commaa separated tag names.")
  private String _tags = "";

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute()
      throws Exception {
    Set<String> includeTableSet = new HashSet<>();
    String[] includeTables = _tables.split(",");
    for (String includeTable : includeTables) {
      String name = stripTypeFromName(includeTable.trim());
      if (name.length() > 0) {
        includeTableSet.add(name);
      }
    }
    Set<String> includeTagSet = new HashSet<>();
    String[] includeTags = _tags.split(",");
    for (String includeTag : includeTags) {
      String name = stripTypeFromName(includeTag.trim());
      if (name.length() > 0) {
        includeTagSet.add(name);
      }
    }

    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo._clusterName = _clusterName;

    ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(_zkAddress);
    if (!zkHelixAdmin.getClusters().contains(_clusterName)) {
      LOGGER.error("Cluster {} not found in {}.", _clusterName, _zkAddress);
      return false;
    }

    List<String> instancesInCluster = zkHelixAdmin.getInstancesInCluster(_clusterName);
    List<String> tables = zkHelixAdmin.getResourcesInCluster(_clusterName);

    ZkClient zkClient = new ZkClient(_zkAddress);
    zkClient.setZkSerializer(new ZNRecordStreamingSerializer());
    LOGGER.info("Connecting to Zookeeper at: {}", _zkAddress);
    zkClient.waitUntilConnected(CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    ZKHelixDataAccessor zkHelixDataAccessor = new ZKHelixDataAccessor(_clusterName, baseDataAccessor);
    PropertyKey property = zkHelixDataAccessor.keyBuilder().liveInstances();
    List<String> liveInstances = zkHelixDataAccessor.getChildNames(property);

    PropertyKey controllerLeaderKey = zkHelixDataAccessor.keyBuilder().controllerLeader();
    LiveInstance controllerLeaderLiveInstance = zkHelixDataAccessor.getProperty(controllerLeaderKey);
    ControllerInfo controllerInfo = new ControllerInfo();
    controllerInfo._leaderName = controllerLeaderLiveInstance.getId();
    clusterInfo._controllerInfo = controllerInfo;
    for (String server : instancesInCluster) {
      if (server.startsWith("Server")) {
        ServerInfo serverInfo = new ServerInfo();
        serverInfo._name = server;
        serverInfo._state = (liveInstances.contains(server)) ? "ONLINE" : "OFFLINE";
        InstanceConfig config = zkHelixAdmin.getInstanceConfig(_clusterName, server);
        serverInfo._tags = config.getRecord().getListField("TAG_LIST");
        clusterInfo.addServerInfo(serverInfo);
      }
      if (server.startsWith("Broker")) {
        BrokerInfo brokerInfo = new BrokerInfo();
        brokerInfo._name = server;
        brokerInfo._state = (liveInstances.contains(server)) ? "ONLINE" : "OFFLINE";
        InstanceConfig config = zkHelixAdmin.getInstanceConfig(_clusterName, server);
        brokerInfo._tags = config.getRecord().getListField("TAG_LIST");
        clusterInfo.addBrokerInfo(brokerInfo);
      }
    }
    for (String table : tables) {
      // Skip non-table resources
      if (!TableNameBuilder.isTableResource(table)) {
        continue;
      }

      TableInfo tableInfo = new TableInfo();
      IdealState idealState = zkHelixAdmin.getResourceIdealState(_clusterName, table);
      ExternalView externalView = zkHelixAdmin.getResourceExternalView(_clusterName, table);
      Set<String> segmentsFromIdealState = idealState.getPartitionSet();

      tableInfo._tableName = table;
      tableInfo._tag = idealState.getRecord().getSimpleField("INSTANCE_GROUP_TAG");
      String rawTableName = stripTypeFromName(tableInfo._tableName);
      String rawTagName = stripTypeFromName(tableInfo._tag);

      if (!includeTableSet.isEmpty() && !includeTableSet.contains(rawTableName)) {
        continue;
      }

      if (!includeTagSet.isEmpty() && !includeTagSet.contains(rawTagName)) {
        continue;
      }
      for (String segment : segmentsFromIdealState) {
        SegmentInfo segmentInfo = new SegmentInfo();
        segmentInfo._name = segment;
        Map<String, String> serverStateMapFromIS = idealState.getInstanceStateMap(segment);
        if (serverStateMapFromIS == null) {
          LOGGER.info("Unassigned segment {} in ideal state", segment);
          serverStateMapFromIS = Collections.emptyMap();
        }
        Map<String, String> serverStateMapFromEV = externalView.getStateMap(segment);
        if (serverStateMapFromEV == null) {
          LOGGER.info("Unassigned segment {} in external view", segment);
          serverStateMapFromEV = Collections.emptyMap();
        }

        for (String serverName : serverStateMapFromIS.keySet()) {
          segmentInfo._segmentStateMap.put(serverName, serverStateMapFromEV.get(serverName));
        }
        tableInfo.addSegmentInfo(segmentInfo);
      }
      clusterInfo.addTableInfo(tableInfo);
    }
    Yaml yaml = new Yaml();
    StringWriter sw = new StringWriter();
    yaml.dump(clusterInfo, sw);
    LOGGER.info(sw.toString());
    return true;
  }

  private String stripTypeFromName(String tableName) {
    return tableName.replace("_OFFLINE", "").replace("_REALTIME", "");
  }

  @Override
  public String description() {
    return "Show Pinot Cluster information.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return ("ShowClusterInfo -clusterName " + _clusterName + " -zkAddress " + _zkAddress + " -tables " + _tables
        + " -tags " + _tags);
  }

  class SegmentInfo {
    @JsonProperty("name")
    public String _name;
    @JsonProperty("segmentStateMap")
    public Map<String, String> _segmentStateMap = new HashMap<String, String>();
  }

  class TableInfo {
    @JsonProperty("tableName")
    public String _tableName;
    @JsonProperty("tag")
    public String _tag;
    @JsonProperty("segmentInfoList")
    public List<SegmentInfo> _segmentInfoList = new ArrayList<SegmentInfo>();

    public void addSegmentInfo(SegmentInfo segmentInfo) {
      _segmentInfoList.add(segmentInfo);
    }
  }

  class ServerInfo {
    @JsonProperty("name")
    public String _name;
    @JsonProperty("tags")
    public List<String> _tags;
    @JsonProperty("state")
    public String _state;
  }

  class BrokerInfo {
    @JsonProperty("name")
    public String _name;
    @JsonProperty("tags")
    public List<String> _tags;
    @JsonProperty("state")
    public String _state;
  }

  class ControllerInfo {
    @JsonProperty("leaderName")
    public String _leaderName;
  }

  class ClusterInfo {
    @JsonProperty("controllerInfo")
    public ControllerInfo _controllerInfo;
    @JsonProperty("brokerInfoList")
    public List<BrokerInfo> _brokerInfoList = new ArrayList<>();
    @JsonProperty("serverInfoList")
    public List<ServerInfo> _serverInfoList = new ArrayList<>();
    @JsonProperty("tableInfoList")
    public List<TableInfo> _tableInfoList = new ArrayList<>();
    @JsonProperty("clusterName")
    public String _clusterName;

    public void addServerInfo(ServerInfo serverInfo) {
      _serverInfoList.add(serverInfo);
    }

    public void addTableInfo(TableInfo tableInfo) {
      _tableInfoList.add(tableInfo);
    }

    public void addBrokerInfo(BrokerInfo brokerInfo) {
      _brokerInfoList.add(brokerInfo);
    }
  }
}
