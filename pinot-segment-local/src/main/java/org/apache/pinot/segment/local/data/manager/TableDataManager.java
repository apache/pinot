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
package org.apache.pinot.segment.local.data.manager;

import com.google.common.cache.LoadingCache;
import java.io.File;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.Pair;


/**
 * The <code>TableDataManager</code> interface provides APIs to manage segments under a table.
 */
@ThreadSafe
public interface TableDataManager {

  /**
   * Initializes the table data manager. Should be called only once and before calling any other method.
   */
  void init(TableDataManagerConfig tableDataManagerConfig, String instanceId, ZkHelixPropertyStore<ZNRecord> propertyStore,
      ServerMetrics serverMetrics, HelixManager helixManager, LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache);

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
  void addSegment(ImmutableSegment immutableSegment);

  /**
   * Adds a segment from local disk into the OFFLINE table.
   */
  void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception;

  /**
   * Adds a segment into the REALTIME table.
   * <p>The segment could be committed or under consuming.
   */
  void addSegment(String segmentName, TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig)
      throws Exception;

  /**
   * Removes a segment from the table.
   */
  void removeSegment(String segmentName);

  /**
   * Acquires all segments of the table.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @return List of segment data managers
   */
  List<SegmentDataManager> acquireAllSegments();

  /**
   * Acquires the segments with the given segment names.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @param segmentNames List of names of the segment to acquire
   * @return List of segment data managers
   */
  List<SegmentDataManager> acquireSegments(List<String> segmentNames);

  /**
   * Acquires the segments with the given segment name.
   * <p>It is the caller's responsibility to return the segments by calling {@link #releaseSegment(SegmentDataManager)}.
   *
   * @param segmentName Name of the segment to acquire
   * @return Segment data manager with the given name, or <code>null</code> if no segment matches the name
   */
  @Nullable
  SegmentDataManager acquireSegment(String segmentName);

  /**
   * Releases the acquired segment.
   *
   * @param segmentDataManager Segment data manager
   */
  void releaseSegment(SegmentDataManager segmentDataManager);

  /**
   * Returns the number of segments managed by this instance.
   */
  int getNumSegments();

  /**
   * Returns the table name managed by this instance.
   */
  String getTableName();

  /**
   * Returns the dir which contains the data segments.
   */
  File getTableDataDir();

  /**
   * Add error related to segment, if any. The implementation
   * is expected to cache last 'N' errors for the table, related to
   * segment transitions.
   */
  void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo);

  /**
   * Returns a list of segment errors that were encountered
   * and cached.
   *
   * @return List of {@link SegmentErrorInfo}
   */
  Map<String, SegmentErrorInfo> getSegmentErrors();
}
