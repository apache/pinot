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

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>ClusterInfoProvider</code> is an abstraction on top of {@link PinotHelixResourceManager} and
 * {@link PinotHelixTaskResourceManager} which provides cluster information for {@link PinotTaskGenerator}.
 */
public class ClusterInfoProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoProvider.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  public ClusterInfoProvider(@Nonnull PinotHelixResourceManager pinotHelixResourceManager,
      @Nonnull PinotHelixTaskResourceManager pinotHelixTaskResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
  }

  @Nullable
  public IdealState getTableIdealState(@Nonnull String tableName) {
    // TODO: add implementation
    throw new UnsupportedOperationException();
  }

  @Nullable
  public AbstractTableConfig getTableConfig(@Nonnull String tableName) {
    return _pinotHelixResourceManager.getTableConfig(tableName);
  }

  @Nullable
  public Schema getTableSchema(@Nonnull String tableName) {
    // TODO: add implementation
    throw new UnsupportedOperationException();
  }

  @Nullable
  public List<SegmentZKMetadata> getSegmentsMetadata(@Nonnull String tableName) {
    // TODO: add implementation
    throw new UnsupportedOperationException();
  }

  @Nonnull
  public Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  @Nonnull
  public PinotTaskConfig getTaskConfig(@Nonnull String taskName) {
    return _pinotHelixTaskResourceManager.getTaskConfig(taskName);
  }
}
