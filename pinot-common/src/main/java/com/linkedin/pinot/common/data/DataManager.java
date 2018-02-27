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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import java.io.File;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


// TODO: maybe better to merge APIs for adding segment to OFFLINE/REALTIME table
public interface DataManager {
  void init(Configuration config, ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics);

  void start();

  void shutDown();

  /**
   * Add a segment from local disk into the OFFLINE table.
   */
  void addOfflineSegment(@Nonnull String offlineTableName, @Nonnull String segmentName, @Nonnull File indexDir)
      throws Exception;

  /**
   * Add a segment into the REALTIME table.
   * <p>The segment could be committed or under consuming.
   */
  void addRealtimeSegment(@Nonnull String realtimeTableName, @Nonnull String segmentName) throws Exception;

  void removeSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName) throws Exception;

  void reloadSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName) throws Exception;

  void reloadAllSegments(@Nonnull String tableNameWithType) throws Exception;

  @Nullable
  SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName);

  @Nonnull
  List<SegmentMetadata> getAllSegmentsMetadata(@Nonnull String tableNameWithType);

  @Nonnull
  String getSegmentDataDirectory();

  @Nonnull
  String getSegmentFileDirectory();

  int getMaxParallelRefreshThreads();
}
