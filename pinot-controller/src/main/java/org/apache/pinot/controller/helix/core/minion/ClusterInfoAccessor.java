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
package org.apache.pinot.controller.helix.core.minion;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.BaseTaskMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>ClusterInfoProvider</code> is an abstraction on top of {@link PinotHelixResourceManager} and
 * {@link PinotHelixTaskResourceManager} which provides cluster information for PinotTaskGenerator.
 */
public class ClusterInfoAccessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoAccessor.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final Executor _executor;
  private final PoolingHttpClientConnectionManager _connectionManager;

  public ClusterInfoAccessor(PinotHelixResourceManager pinotHelixResourceManager,
      PinotHelixTaskResourceManager pinotHelixTaskResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager, Executor executor,
      PoolingHttpClientConnectionManager connectionManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * Get the table config for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getTableConfig(String tableNameWithType) {
    return _pinotHelixResourceManager.getTableConfig(tableNameWithType);
  }

  /**
   * Get the table schema for the given table name with or without type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table schema
   */
  @Nullable
  public Schema getTableSchema(String tableName) {
    return _pinotHelixResourceManager.getTableSchema(tableName);
  }

  /**
   * Get all segments' ZK metadata for the given table.
   *
   * @param tableNameWithType Table name with type suffix
   * @return List of segments' ZK metadata
   */
  public List<SegmentZKMetadata> getSegmentsZKMetadata(String tableNameWithType) {
    return ZKMetadataProvider.getSegmentsZKMetadata(_pinotHelixResourceManager.getPropertyStore(), tableNameWithType);
  }

  public void forEachSegmentsZKMetadata(String tableNameWithType, int batchSize,
      Consumer<SegmentZKMetadata> segmentMetadataConsumer) {
    _pinotHelixResourceManager.forEachSegmentsZKMetadata(tableNameWithType, batchSize, segmentMetadataConsumer);
  }

  public void forEachSegmentsZKMetadata(String tableNameWithType, Consumer<SegmentZKMetadata> segmentMetadataConsumer) {
    _pinotHelixResourceManager.forEachSegmentsZKMetadata(tableNameWithType, segmentMetadataConsumer);
  }

  public IdealState getIdealState(String tableNameWithType) {
    return _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
  }

  /**
   * Get shared executor
   */
  public Executor getExecutor() {
    return _executor;
  }

  /**
   * Get shared connection manager
   */
  public PoolingHttpClientConnectionManager getConnectionManager() {
    return _connectionManager;
  }

  /**
   * Fetches the ZNRecord under MINION_TASK_METADATA/${tableNameWithType}/${taskType} for the given
   * taskType and tableNameWithType
   *
   * @param taskType The type of the minion task
   * @param tableNameWithType Table name with type
   */
  public ZNRecord getMinionTaskMetadataZNRecord(String taskType, String tableNameWithType) {
    return MinionTaskMetadataUtils.fetchTaskMetadata(_pinotHelixResourceManager.getPropertyStore(), taskType,
        tableNameWithType);
  }

  /**
   * Get the segment lineage for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Segment lineage
   */
  @Nullable
  public SegmentLineage getSegmentLineage(String tableNameWithType) {
    return SegmentLineageAccessHelper.getSegmentLineage(_pinotHelixResourceManager.getPropertyStore(),
        tableNameWithType);
  }

  /**
   * Sets a minion task metadata into MINION_TASK_METADATA
   * This call will override any previous metadata node
   *
   * @param taskMetadata The task metadata to persist
   * @param taskType The type of the minion task
   * @param expectedVersion The expected version of data to be overwritten. Set to -1 to override version check.
   */
  public void setMinionTaskMetadata(BaseTaskMetadata taskMetadata, String taskType, int expectedVersion) {
    MinionTaskMetadataUtils.persistTaskMetadata(_pinotHelixResourceManager.getPropertyStore(), taskType, taskMetadata,
        expectedVersion);
  }

  /**
   * Get all tasks' state for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  public Map<String, TaskState> getTaskStates(String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  /**
   * Get the child task configs for the given task name.
   *
   * @param taskName Task name
   * @return List of child task configs
   */
  public List<PinotTaskConfig> getTaskConfigs(String taskName) {
    return _pinotHelixTaskResourceManager.getSubtaskConfigs(taskName);
  }

  /**
   * Get the VIP URL for the controllers.
   *
   * @return VIP URL
   */
  public String getVipUrl() {
    return _controllerConf.generateVipUrl();
  }

  /**
   * Get the VIP URL of the lead controller for the given table.
   * Falls back to the current controller's VIP URL if lead controller cannot be determined for some reason
   *
   * @param tableNameWithType table name with type
   * @return VIP URL of the lead controller of the table
   */
  public String getVipUrlForLeadController(String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    if (_leadControllerManager.isLeaderForTable(rawTableName)) {
      LOGGER.info("Controller is leader for table {}", tableNameWithType);
      return getVipUrl();
    }

    String leadControllerInstanceId = getLeadControllerForTable(tableNameWithType);
    if (leadControllerInstanceId == null) {
      LOGGER.warn("Lead controller instance ID returned is null for table: {}, setting this controller's VIP "
              + "URL instead", tableNameWithType);
      return getVipUrl();
    }

    // Fetch the instance config for the given controller and obtain the hostname and port
    HelixDataAccessor dataAccessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    InstanceConfig instanceConfig = dataAccessor.getProperty(keyBuilder.instanceConfig(leadControllerInstanceId));
    if (instanceConfig == null) {
      LOGGER.warn("Instance config is null for lead controller instance: {} for table: {}, setting this controller's "
              + "VIP URL instead", leadControllerInstanceId, tableNameWithType);
      return getVipUrl();
    }

    String host = instanceConfig.getHostName();
    String port = instanceConfig.getPort();
    return _controllerConf.generateVipUrl(host, port);
  }

  private String getLeadControllerForTable(String tableNameWithType) {
    try {
      HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();

      // Check if lead controller resource is enabled
      boolean isLeadControllerResourceEnabled = LeadControllerUtils.isLeadControllerResourceEnabled(helixManager);
      if (!isLeadControllerResourceEnabled) {
        LOGGER.warn("Lead controller resource is disabled, returning null");
        return null;
      }

      // Get external view for lead controller resource
      ExternalView leadControllerResourceExternalView = helixManager.getClusterManagmentTool()
          .getResourceExternalView(helixManager.getClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);

      if (leadControllerResourceExternalView == null) {
        LOGGER.warn("Lead controller resource external view is null, returning null");
        return null;
      }

      // Find the partition for this table
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);

      // Get the state map for this partition
      Map<String, String> partitionStateMap = leadControllerResourceExternalView.getStateMap(partitionName);
      if (partitionStateMap == null) {
        LOGGER.warn("Lead controller resource EV's partition state map is null for table: {}, returning null",
            tableNameWithType);
        return null;
      }

      // Find the controller in MASTER state
      for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
        if (MasterSlaveSMD.States.MASTER.name().equals(entry.getValue())) {
          return entry.getKey();
        }
      }

      LOGGER.warn("Could not find the lead controller for table: {} in MASTER state, returning null",
          tableNameWithType);
      return null;
    } catch (Exception e) {
      LOGGER.warn("Caught exception while trying to fetch the lead controller for table: {}", tableNameWithType, e);
      return null;
    }
  }

  /**
   * Get the data dir.
   *
   * @return the data dir.
   */
  public String getDataDir() {
    return _controllerConf.getDataDir();
  }

  /**
   * Get the cluster config for a given config name, return null if not found.
   *
   * @return cluster config
   */
  public String getClusterConfig(String configName) {
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
            _pinotHelixResourceManager.getHelixClusterName()).build();
    Map<String, String> configMap =
        _pinotHelixResourceManager.getHelixAdmin().getConfig(helixConfigScope, Collections.singletonList(configName));
    return configMap != null ? configMap.get(configName) : null;
  }

  /**
   * Get the controller metrics.
   *
   * @return controller metrics
   */
  public ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  /**
   * Get the leader controller manager.
   *
   * @return leader controller manager
   */
  public LeadControllerManager getLeaderControllerManager() {
    return _leadControllerManager;
  }

  /**
   * Get the helix resource manager for minion task generator to
   * access the info of tables, segments, instances, etc.
   *
   * @return helix resource manager
   */
  public PinotHelixResourceManager getPinotHelixResourceManager() {
    return _pinotHelixResourceManager;
  }

  /**
   * Get the helix task resource manager for minion task generator to
   * access the info of minion tasks.
   *
   * @return helix task resource manager
   */
  public PinotHelixTaskResourceManager getPinotHelixTaskResourceManager() {
    return _pinotHelixTaskResourceManager;
  }
}
