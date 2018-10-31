/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.manager;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import java.io.File;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * The <code>InstanceDataManager</code> class is the instance level data manager, which manages all tables and segments
 * served by the instance.
 */
@ThreadSafe
public interface InstanceDataManager {

  /**
   * Initializes the data manager.
   * <p>Should be called only once and before calling any other method.
   */
  void init(@Nonnull Configuration config, @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull ServerMetrics serverMetrics) throws ConfigurationException;

  /**
   * Starts the data manager.
   * <p>Should be called only once after data manager gets initialized but before calling any other method.
   */
  void start();

  /**
   * Shuts down the data manager.
   * <p>Should be called only once. After calling shut down, no other method should be called.
   */
  void shutDown();

  /**
   * Adds a segment from local disk into an OFFLINE table.
   */
  void addOfflineSegment(@Nonnull String offlineTableName, @Nonnull String segmentName, @Nonnull File indexDir)
      throws Exception;

  /**
   * Adds a segment into an REALTIME table.
   * <p>The segment might be committed or under consuming.
   */
  void addRealtimeSegment(@Nonnull String realtimeTableName, @Nonnull String segmentName) throws Exception;

  /**
   * Removes a segment from a table.
   */
  void removeSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName) throws Exception;

  /**
   * Reloads a segment in a table.
   */
  void reloadSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName) throws Exception;

  /**
   * Reloads all segment in a table.
   */
  void reloadAllSegments(@Nonnull String tableNameWithType) throws Exception;

  /**
   * Returns all tables served by the instance.
   */
  @Nonnull
  Set<String> getAllTables();

  /**
   * Returns the table data manager for the given table, or <code>null</code> if it does not exist.
   */
  @Nullable
  TableDataManager getTableDataManager(@Nonnull String tableNameWithType);

  /**
   * Returns the segment metadata for the given segment in the given table, or <code>null</code> if it does not exist.
   */
  @Nullable
  SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName);

  /**
   * Returns the metadata for all segments in the given table.
   */
  @Nonnull
  List<SegmentMetadata> getAllSegmentsMetadata(@Nonnull String tableNameWithType);

  /**
   * Returns the directory for un-tarred segment data.
   */
  @Nonnull
  String getSegmentDataDirectory();

  /**
   * Returns the directory for tarred segment file.
   */
  @Nonnull
  String getSegmentFileDirectory();

  /**
   * Returns the maximum number of segments allowed to refresh in parallel.
   */
  int getMaxParallelRefreshThreads();
}
