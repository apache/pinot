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
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
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

  public ClusterInfoProvider(@Nonnull PinotHelixResourceManager pinotHelixResourceManager,
      @Nonnull PinotHelixTaskResourceManager pinotHelixTaskResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
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
   * Get the table schema for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Table schema
   */
  @Nullable
  public Schema getTableSchema(@Nonnull String tableNameWithType) {
    return _pinotHelixResourceManager.getTableSchema(tableNameWithType);
  }

  /**
   * Get all segment metadata for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return List of segment metadata
   */
  @Nonnull
  public List<? extends SegmentZKMetadata> getSegmentsMetadata(@Nonnull String tableNameWithType) {
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    Preconditions.checkNotNull(tableType);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      return ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(),
          tableNameWithType);
    } else {
      return ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(_pinotHelixResourceManager.getPropertyStore(),
          tableNameWithType);
    }
  }

  /**
   * Get all task states for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  @Nonnull
  public Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  /**
   * Get the task config for the given task name.
   *
   * @param taskName Task name
   * @return Task config
   */
  @Nonnull
  public PinotTaskConfig getTaskConfig(@Nonnull String taskName) {
    return _pinotHelixTaskResourceManager.getTaskConfig(taskName);
  }
}
