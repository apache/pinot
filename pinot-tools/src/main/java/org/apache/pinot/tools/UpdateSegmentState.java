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
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(usageHelpAutoWidth = true, description =
    "Audit the IDEALSTATE for the segments of a table (or all tables of a tenant). Optionally update segment "
    + "state from OFFLINE to ONLINE")
public class UpdateSegmentState extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateSegmentState.class);
  private static final String CMD_NAME = "UpdateSegmentState";
  private static final String FROM_STATE = "OFFLINE";
  private static final String TO_STATE = "ONLINE";
  static final String DEFAULT_ZK_ADDRESS = "localhost:2181";
  static final String DEFAULT_CLUSTER_NAME = "PinotCluster";

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @CommandLine.Option(names = {"-tenantName"}, required = false, description = "Name of tenant.")
  private String _tenantName;

  @CommandLine.Option(names = {"-tableName"}, required = false,
      description = "Name of the table (e.g. foo_table_OFFLINE).")
  private String _tableName;

  @CommandLine.Option(names = {"-fix"}, required = false, description = "Update IDEALSTATE values (OFFLINE->ONLINE).")
  private boolean _fix = false;

  public UpdateSegmentState() {
    super();
  }

  @Override
  public String getName() {
    return CMD_NAME;
  }

  @Override
  public String toString() {
    String retString = CMD_NAME + " -zkAddress " + _zkAddress + " -clusterName " + _clusterName;
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
    String path = PropertyPathBuilder.propertyStore(_clusterName);
    _propertyStore = new ZkHelixPropertyStore<>(_zkAddress, serializer, path);
  }

  public List<String> getAllTenantTables()
      throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE";
    List<ZNRecord> tableConfigs = _propertyStore
        .getChildren(tableConfigPath, null, 0, CommonConstants.Helix.ZkClient.RETRY_COUNT,
            CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
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
        if (state.equals(FROM_STATE)) {
          if (_fix) {
            mapIS.put(server, TO_STATE);
          } else {
            LOGGER.info("Table:" + tableName + ",Segment:" + segment + ",Server:" + server + ":" + FROM_STATE);
          }
          nChanges++;
        }
      }
    }
    if (nChanges == 0) {
      LOGGER.info("No segments detected in " + FROM_STATE + " state for table " + tableName);
    } else {
      if (_fix) {
        LOGGER.info("Replacing IDEALSTATE for table " + tableName + " with " + nChanges + " changes");
        _helixAdmin.setResourceIdealState(_clusterName, tableName, idealState);
      } else {
        LOGGER.info("Detected " + nChanges + " instances in " + FROM_STATE + " in table " + tableName);
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
      if (!tableNames.isEmpty()) {
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
