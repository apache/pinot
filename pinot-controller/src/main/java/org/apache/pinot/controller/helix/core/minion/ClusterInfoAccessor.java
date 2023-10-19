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
import javax.annotation.Nullable;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.BaseTaskMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * The class <code>ClusterInfoProvider</code> is an abstraction on top of {@link PinotHelixResourceManager} and
 * {@link PinotHelixTaskResourceManager} which provides cluster information for PinotTaskGenerator.
 */
public class ClusterInfoAccessor {
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
