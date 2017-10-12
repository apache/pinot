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

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * The <code>TableDataManager</code> interface provides APIs to manage segments under a table.
 */
public interface TableDataManager {

  void init(@Nonnull TableDataManagerConfig tableDataManagerConfig, @Nonnull String instanceId,
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull ServerMetrics serverMetrics);

  void start();

  void shutDown();

  /**
   * Adds a loaded {@link IndexSegment} into the table.
   */
  void addSegment(@Nonnull IndexSegment indexSegment);

  /**
   * Adds a segment from local disk into the OFFLINE table.
   */
  void addSegment(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig) throws Exception;

  /**
   * Adds a segment into the REALTIME table.
   * <p>The segment could be committed or under consuming.
   */
  void addSegment(@Nonnull String segmentName, @Nonnull TableConfig tableConfig,
      @Nonnull IndexLoadingConfig indexLoadingConfig) throws Exception;

  /**
   * Removes a segment from the table.
   */
  void removeSegment(String segmentName);

  /**
   *
   * @note This method gets a lock on the segments. It is the caller's responsibility to return the segments
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method
   * @return segments by giving a list of segment names in this TableDataManager.
   */
  @Nonnull
  ImmutableList<SegmentDataManager> acquireAllSegments();

  /**
   *
   * @note This method gets a lock on the segments. It is the caller's responsibility to return the segments
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method
   * @return segments by giving a list of segment names in this TableDataManager.
   */
  List<SegmentDataManager> acquireSegments(List<String> segmentList);

  /**
   *
   * @note This method gets a lock on the segment. It is the caller's responsibility to return the segment
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method.
   * @return a segment by giving the name of this segment in this TableDataManager.
   */
  @Nullable
  SegmentDataManager acquireSegment(String segmentName);

  /**
   *
   * give back segmentReader, so the segment could be safely deleted.
   */
  void releaseSegment(SegmentDataManager segmentDataManager);

  /**
   * Get the table name managed by this instance
   */
  String getTableName();
}
