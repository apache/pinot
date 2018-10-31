/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.minion;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.task.TaskState;


/**
 * The class <code>ClusterInfoProvider</code> is an abstraction on top of {@link PinotHelixResourceManager} and
 * {@link PinotHelixTaskResourceManager} which provides cluster information for {@link PinotTaskGenerator}.
 */
public class ClusterInfoProvider {
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private final ControllerConf _controllerConf;

  public ClusterInfoProvider(@Nonnull PinotHelixResourceManager pinotHelixResourceManager,
      @Nonnull PinotHelixTaskResourceManager pinotHelixTaskResourceManager, @Nonnull ControllerConf controllerConf) {
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
  public TableConfig getTableConfig(@Nonnull String tableNameWithType) {
    return _pinotHelixResourceManager.getTableConfig(tableNameWithType);
  }

  /**
   * Get the table schema for the given table name with or without type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table schema
   */
  @Nullable
  public Schema getTableSchema(@Nonnull String tableName) {
    return _pinotHelixResourceManager.getTableSchema(tableName);
  }

  /**
   * Get all segments' metadata for the given OFFLINE table name.
   *
   * @param offlineTableName Offline table name
   * @return List of segments' metadata
   */
  @Nonnull
  public List<OfflineSegmentZKMetadata> getOfflineSegmentsMetadata(@Nonnull String offlineTableName) {
    Preconditions.checkArgument(TableNameBuilder.OFFLINE.tableHasTypeSuffix(offlineTableName));
    return ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(),
        offlineTableName);
  }

  /**
   * Get all segments' metadata for the given REALTIME table name.
   *
   * @param realtimeTableName Realtime table name
   * @return List of segments' metadata
   */
  @Nonnull
  public List<RealtimeSegmentZKMetadata> getRealtimeSegmentsMetadata(@Nonnull String realtimeTableName) {
    Preconditions.checkArgument(TableNameBuilder.REALTIME.tableHasTypeSuffix(realtimeTableName));
    return ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(),
        realtimeTableName);
  }

  /**
   * Get all tasks' state for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  @Nonnull
  public Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  /**
   * Get the child task configs for the given task name.
   *
   * @param taskName Task name
   * @return List of child task configs
   */
  @Nonnull
  public List<PinotTaskConfig> getTaskConfigs(@Nonnull String taskName) {
    return _pinotHelixTaskResourceManager.getTaskConfigs(taskName);
  }

  /**
   * Get the VIP URL for the controllers.
   *
   * @return VIP URL
   */
  @Nonnull
  public String getVipUrl() {
    return _controllerConf.generateVipUrl();
  }
}
