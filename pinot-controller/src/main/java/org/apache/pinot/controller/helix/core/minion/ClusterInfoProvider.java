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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * The class <code>ClusterInfoProvider</code> is an abstraction on top of {@link PinotHelixResourceManager} and
 * {@link PinotHelixTaskResourceManager} which provides cluster information for {@link PinotTaskGenerator}.
 */
public class ClusterInfoProvider {
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private final ControllerConf _controllerConf;

  public ClusterInfoProvider(PinotHelixResourceManager pinotHelixResourceManager,
      PinotHelixTaskResourceManager pinotHelixTaskResourceManager, ControllerConf controllerConf) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
    _controllerConf = controllerConf;
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
   * Get all segments' metadata for the given OFFLINE table name.
   *
   * @param tableName Table name with or without OFFLINE type suffix
   * @return List of segments' metadata
   */
  public List<OfflineSegmentZKMetadata> getOfflineSegmentsMetadata(String tableName) {
    return ZKMetadataProvider
        .getOfflineSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(), tableName);
  }

  /**
   * Get all segments' metadata for the given REALTIME table name.
   *
   * @param tableName Table name with or without REALTIME type suffix
   * @return List of segments' metadata
   */
  public List<RealtimeSegmentZKMetadata> getRealtimeSegmentsMetadata(String tableName) {
    return ZKMetadataProvider
        .getRealtimeSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(), tableName);
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
    return _pinotHelixTaskResourceManager.getTaskConfigs(taskName);
  }

  /**
   * Get the VIP URL for the controllers.
   *
   * @return VIP URL
   */
  public String getVipUrl() {
    return _controllerConf.generateVipUrl();
  }
}
