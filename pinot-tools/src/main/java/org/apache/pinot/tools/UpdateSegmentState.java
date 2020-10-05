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
package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpdateSegmentState extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateSegmentState.class);
  private static final String CmdName = "UpdateSegmentState";
  private static final String fromState = "OFFLINE";
  private static final String toState = "ONLINE";
  static final String DEFAULT_ZK_ADDRESS = "localhost:2181";
  static final String DEFAULT_CLUSTER_NAME = "PinotCluster";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @Option(name = "-tenantName", required = false, metaVar = "<string>", usage = "Name of tenant.")
  private String _tenantName;

  @Option(name = "-tableName", required = false, metaVar = "<string>", usage = "Name of the table (e.g. foo_table_OFFLINE).")
  private String _tableName;

  @Option(name = "-fix", required = false, metaVar = "<boolean>", usage = "Update IDEALSTATE values (OFFLINE->ONLINE).")
  private boolean _fix = false;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public UpdateSegmentState() {
    super();
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return CmdName;
  }

  @Override
  public String toString() {
    String retString = CmdName + " -zkAddress " + _zkAddress + " -clusterName " + _clusterName;
    if (_tableName != null) {
      retString += " -tableName " + _tableName;
    } else {
      retString += " -tenanName " + _tenantName;
    }
    if (_fix) {
      retString += " -fix";
    }
    return retString;
  }

  @Override
  public String description() {
    return "Audit the IDEALSTATE for the segments of a table (or all tables of a tenant). Optionally update segment state from OFFLINE to ONLINE";
  }

  public UpdateSegmentState setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public UpdateSegmentState setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public UpdateSegmentState setTenantName(String tenantName) {
    _tenantName = tenantName;
    return this;
  }

  public UpdateSegmentState setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public UpdateSegmentState setOverwrite(boolean fix) {
    _fix = fix;
    return this;
  }

  private ZKHelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private void init() {
    LOGGER.info("Trying to connect to " + _zkAddress + " cluster " + _clusterName);
    _helixAdmin = new ZKHelixAdmin(_zkAddress);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);
    _propertyStore = new ZkHelixPropertyStore<>(_zkAddress, serializer, path);
  }

  public List<String> getAllTenantTables()
      throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE";
    List<ZNRecord> tableConfigs = _propertyStore.getChildren(tableConfigPath, null, 0,
        CommonConstants.Helix.ZkClient.RETRY_COUNT, CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    List<String> tables = new ArrayList<>(128);
    for (ZNRecord znRecord : tableConfigs) {
      TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
      if (tableConfig.getTenantConfig().getServer().equals(_tenantName)) {
        tables.add(tableConfig.getTableName());
      }
    }
    return tables;
  }

  public void fixTableIdealState(String tableName)
      throws Exception {
    IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, tableName);
    if (idealState == null) {
      LOGGER.info("No IDEALSTATE found for table " + tableName);
      return;
    }
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    int nChanges = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (state.equals(fromState)) {
          if (_fix) {
            mapIS.put(server, toState);
          } else {
            LOGGER.info("Table:" + tableName + ",Segment:" + segment + ",Server:" + server + ":" + fromState);
          }
          nChanges++;
        }
      }
    }
    if (nChanges == 0) {
      LOGGER.info("No segments detected in " + fromState + " state for table " + tableName);
    } else {
      if (_fix) {
        LOGGER.info("Replacing IDEALSTATE for table " + tableName + " with " + nChanges + " changes");
        _helixAdmin.setResourceIdealState(_clusterName, tableName, idealState);
      } else {
        LOGGER.info("Detected " + nChanges + " instances in " + fromState + " in table " + tableName);
      }
    }
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_tableName == null && _tenantName == null) {
      LOGGER.error("One of -tableName or -tenantName must be specified.");
      return false;
    }
    if (_tableName != null && _tenantName != null) {
      LOGGER.error("Exactly one of -tenantName and -tableName be specified");
      return false;
    }
    init();

    if (_tenantName != null) {
      // Do this for all tenant tables
      LOGGER.info("Working on all tables for tenant " + _tenantName);
      List<String> tableNames = getAllTenantTables();
      LOGGER.info("Found " + tableNames.size() + " tables for tenant " + _tenantName);
      if (tableNames.size() > 0) {
        for (String tableName : tableNames) {
          fixTableIdealState(tableName);
        }
      }
    } else {
      LOGGER.info("Working on table " + _tableName);
      fixTableIdealState(_tableName);
    }
    return true;
  }
}
