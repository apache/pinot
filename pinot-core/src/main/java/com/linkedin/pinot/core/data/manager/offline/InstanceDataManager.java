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
package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadata;
import com.linkedin.pinot.core.metadata.segment.SegmentZKMetadata;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public interface InstanceDataManager {

  void init(@Nonnull Configuration instanceDataManagerConfig);

  void start();

  boolean isStarted();

  @Nonnull
  String getInstanceDataDir();

  @Nonnull
  String getInstanceSegmentTarDir();

  @Nullable
  TableDataManager getTableDataManager(String tableNameWithType);

  @Nonnull
  Collection<TableDataManager> getTableDataManagers();

  /**
   * Add a segment from local disk into an OFFLINE table
   */
  void addSegment(@Nonnull SegmentMetadata segmentMetadata, @Nullable TableConfig tableConfig, @Nullable Schema schema)
      throws Exception;

  /**
   * Add a segment into a REALTIME table
   * <p>The segment could be committed or under consuming
   */
  void addSegment(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull TableConfig tableConfig,
      @Nullable InstanceZKMetadata instanceZKMetadata, @Nonnull SegmentZKMetadata segmentZKMetadata,
      @Nonnull String serverInstance) throws Exception;

  // TODO: add tableName as an argument
  void removeSegment(@Nonnull String segmentName);

  void reloadSegment(@Nonnull String tableNameWithType, @Nonnull SegmentMetadata segmentMetadata,
      @Nullable TableConfig tableConfig, @Nullable Schema schema) throws Exception;

  @Nullable
  SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName);

  @Nonnull
  List<SegmentMetadata> getSegmentsMetadata(@Nonnull String tableNameWithType);

  void shutDown();
}
