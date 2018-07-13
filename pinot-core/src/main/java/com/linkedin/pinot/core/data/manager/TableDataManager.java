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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * The <code>TableDataManager</code> interface provides APIs to manage segments under a table.
 */
@ThreadSafe
public interface TableDataManager {

  /**
   * Initializes the table data manager. Should be called only once and before calling any other method.
   */
  void init(@Nonnull TableDataManagerConfig tableDataManagerConfig, @Nonnull String instanceId,
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull ServerMetrics serverMetrics);

  /**
   * Starts the table data manager. Should be called only once after table data manager gets initialized but before
   * calling any other method.
   */
  void start();

  /**
   * Shuts down the table data manager. Should be called only once. After calling shut down, no other method should be
   * called.
   */
  void shutDown();

  /**
   * Adds a loaded immutable segment into the table.
   */
  void addSegment(@Nonnull ImmutableSegment immutableSegment);

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
  void removeSegment(@Nonnull String segmentName);

  /**
   * Acquires all segments of the table.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @return List of segment data managers
   */
  @Nonnull
  List<SegmentDataManager> acquireAllSegments();

  /**
   * Acquires the segments with the given segment names.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @param segmentNames List of names of the segment to acquire
   * @return List of segment data managers
   */
  @Nonnull
  List<SegmentDataManager> acquireSegments(@Nonnull List<String> segmentNames);

  /**
   * Acquires the segments with the given segment name.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @param segmentName Name of the segment to acquire
   * @return Segment data manager with the given name, or <code>null</code> if no segment matches the name
   */
  SegmentDataManager acquireSegment(@Nonnull String segmentName);

  /**
   * Releases the acquired segment.
   *
   * @param segmentDataManager Segment data manager
   */
  void releaseSegment(@Nonnull SegmentDataManager segmentDataManager);

  /**
   * Returns the table name managed by this instance.
   */
  @Nonnull
  String getTableName();
}
