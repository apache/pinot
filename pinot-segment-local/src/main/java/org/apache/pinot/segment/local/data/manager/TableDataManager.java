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
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * The <code>TableDataManager</code> interface provides APIs to manage segments under a table.
 */
@ThreadSafe
public interface TableDataManager {

  /**
   * Initializes the table data manager. Should be called only once and before calling any other method.
   */
  void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache,
      TableDataManagerParams tableDataManagerParams);

  /**
   * Starts the table data manager. Should be called only once after table data manager gets initialized but before
   * calling any other method.
   */
  void start();

  /**
   * Shuts down the table data manager. After calling shut down, no other method should be called.
   * NOTE: Shut down might be called multiple times. The implementation should be able to handle that.
   */
  void shutDown();

  boolean isShutDown();

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
  void addSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata)
      throws Exception;

  /**
   * Reloads an existing immutable segment for the table, which can be an OFFLINE or REALTIME table.
   * A new segment may be downloaded if the local one has a different CRC; or can be forced to download
   * if forceDownload flag is true. This operation is conducted within a failure handling framework
   * and made transparent to ongoing queries, because the segment is in online serving state.
   *
   * TODO: Clean up this method to use the schema from the IndexLoadingConfig
   *
   * @param segmentName the segment to reload
   * @param indexLoadingConfig the latest table config to load segment
   * @param zkMetadata the segment metadata from zookeeper
   * @param localMetadata the segment metadata object held by server right now,
   *                      which must not be null to reload the segment
   * @param schema the latest table schema to load segment
   * @param forceDownload whether to force to download raw segment to reload
   * @throws Exception thrown upon failure when to reload the segment
   */
  void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception;

  /**
   * Adds or replaces an immutable segment for the table, which can be an OFFLINE or REALTIME table.
   * A new segment may be downloaded if the local one has a different CRC or doesn't work as expected.
   * This operation is conducted outside the failure handling framework as used in segment reloading,
   * because the segment is not yet online serving queries, e.g. this method is used to add a new segment,
   * or transition a segment to online serving state.
   *
   * @param segmentName the segment to add or replace
   * @param indexLoadingConfig the latest table config to load segment
   * @param zkMetadata the segment metadata from zookeeper
   * @param localMetadata the segment metadata object held by server, which can be null when
   *                      the server is restarted or the segment is newly added to the table
   * @throws Exception thrown upon failure when to add or replace the segment
   */
  void addOrReplaceSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      @Nullable SegmentMetadata localMetadata)
      throws Exception;

  /**
   * Removes a segment from the table.
   */
  void removeSegment(String segmentName);

  /**
   * Try to load a segment from an existing segment directory managed by the server. The segment loading may fail
   * because the directory may not exist any more, or the segment data has a different crc now, or the existing segment
   * data got corrupted etc.
   *
   * @return true if the segment is loaded successfully from the existing segment directory; false otherwise.
   */
  boolean tryLoadExistingSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata);

  /**
   * Get the segment data directory, considering the segment tier if provided.
   */
  File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig);

  /**
   * Returns true if the segment was deleted in the last few minutes.
   */
  boolean isSegmentDeletedRecently(String segmentName);

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
   * This method may return some recently deleted segments in missingSegments. The caller can identify those segments
   * by using {@link #isSegmentDeletedRecently(String)}.
   *
   * @param segmentNames List of names of the segment to acquire
   * @param missingSegments Holder for segments unable to be acquired
   * @return List of segment data managers
   */
  List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments);

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
   * Returns the config for the table data manager.
   */
  TableDataManagerConfig getTableDataManagerConfig();

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

  /**
   * Interface to handle segment state transitions from CONSUMING to DROPPED
   *
   * @param segmentNameStr name of segment for which the state change is being handled
   */
  default void onConsumingToDropped(String segmentNameStr) {
  };

  /**
   * Interface to handle segment state transitions from CONSUMING to ONLINE
   *
   * @param segmentNameStr name of segment for which the state change is being handled
   */
  default void onConsumingToOnline(String segmentNameStr) {
  };
}
