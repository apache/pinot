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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
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
  void init(InstanceDataManagerConfig instanceDataManagerConfig, HelixManager helixManager, SegmentLocks segmentLocks,
      TableConfig tableConfig, SegmentReloadSemaphore segmentReloadSemaphore, ExecutorService segmentReloadExecutor,
      @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable Cache<Pair<String, String>, SegmentErrorInfo> errorCache,
      @Nullable SegmentOperationsThrottler segmentOperationsThrottler);

  /**
   * Returns the instance id of the server.
   */
  String getInstanceId();

  /**
   * Returns the config for the instance data manager.
   */
  InstanceDataManagerConfig getInstanceDataManagerConfig();

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
   * Returns the segment lock for a segment in the table.
   */
  Lock getSegmentLock(String segmentName);

  /**
   * Returns whether the segment is loaded in the table.
   */
  boolean hasSegment(String segmentName);

  /**
   * Adds a loaded immutable segment into the table.
   * NOTE: This method is not designed to be directly used by the production code, but can be handy to set up tests.
   */
  @VisibleForTesting
  void addSegment(ImmutableSegment immutableSegment);

  /**
   * Adds an ONLINE segment into a table.
   * This method is triggered by state transition to ONLINE state.
   * If the segment is already loaded:
   * - If the segment is a consuming segment, it will be transitioned to immutable segment (consuming segment commit).
   * - If the segment is already an immutable segment, replace it if its CRC doesn't match the ZK metadata.
   */
  void addOnlineSegment(String segmentName)
      throws Exception;

  /**
   * Adds a new ONLINE segment that is not already loaded.
   * NOTE: This method is part of the implementation detail of {@link #addOnlineSegment(String)}.
   */
  void addNewOnlineSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception;

  /**
   * Tries to load a segment from local disk. Segment load might fail for various reasons: segment doesn't exist on
   * local disk; local segment contains different CRC with ZK metadata; local data got corrupted; etc.
   * NOTE: This method is part of the implementation detail of {@link #addOnlineSegment(String)}.
   *
   * @return true if the segment is loaded successfully from local disk; false otherwise.
   */
  boolean tryLoadExistingSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig);

  /**
   * Check if reload is needed for any of the segments of a table
   * @return true if reload is needed for any of the segments and false otherwise
   */
  boolean needReloadSegments()
      throws Exception;

  /**
   * Downloads a segment and loads it into the table.
   * NOTE: This method is part of the implementation detail of {@link #addOnlineSegment(String)}.
   */
  void downloadAndLoadSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception;

  /**
   * Adds an CONSUMING segment into a REALTIME table.
   * This method is triggered by state transition to CONSUMING state.
   */
  void addConsumingSegment(String segmentName)
      throws Exception;

  /**
   * Replaces an already loaded segment in a table if the segment has been overridden in the deep store (CRC mismatch).
   * This method is triggered by a custom message (NOT state transition), and the target segment should be in ONLINE
   * state.
   */
  void replaceSegment(String segmentName)
      throws Exception;

  /**
   * Offloads a segment from table but not dropping its data from server.
   * This method is triggered by state transition to OFFLINE state.
   */
  void offloadSegment(String segmentName)
      throws Exception;

  /**
   * Offloads a segment from table like the method {@link #offloadSegment(String)}, but this method doesn't take
   * segment lock internally to allow separate threads to manage segment offloading w/o the risk of deadlocks.
   */
  void offloadSegmentUnsafe(String segmentName)
      throws Exception;

  /**
   * Reloads an existing immutable segment for the table, which can be an OFFLINE or REALTIME table.
   * A new segment may be downloaded if the local one has a different CRC; or can be forced to download if
   * {@code forceDownload} flag is true.
   * This operation is conducted within a failure handling framework and made transparent to ongoing queries, because
   * the segment is in online serving state.
   */
  void reloadSegment(String segmentName, boolean forceDownload)
      throws Exception;

  /**
   * Reloads all segments.
   */
  void reloadAllSegments(boolean forceDownload)
      throws Exception;

  /**
   * Reloads a list of segments.
   */
  void reloadSegments(List<String> segmentNames, boolean forceDownload)
      throws Exception;

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

  default List<SegmentDataManager> acquireSegments(List<String> segmentNames,
      @Nullable List<String> optionalSegmentNames, List<String> missingSegments) {
    return acquireSegments(segmentNames, missingSegments);
  }

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
   * Returns helixManager that is used to find out the segments to preload.
   */
  HelixManager getHelixManager();

  /**
   * Returns segmentPreloadExecutor that is used when to preload segments in parallel.
   */
  ExecutorService getSegmentPreloadExecutor();

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
   * Get more context for the selected segments for query execution, e.g. getting the validDocIds bitmaps for segments
   * in upsert tables. This method allows contexts of many segments to be obtained together, making it easier to
   * ensure data consistency across those segments if needed.
   */
  List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments, Map<String, String> queryOptions);

  /**
   * Fetches the segment ZK metadata for the given segment.
   */
  SegmentZKMetadata fetchZKMetadata(String segmentName);

  /**
   * Fetches the table config and schema for the table from ZK.
   */
  Pair<TableConfig, Schema> fetchTableConfigAndSchema();

  /**
   * Fetches the table config and schema for the table from ZK, then construct the index loading config with them.
   */
  default IndexLoadingConfig fetchIndexLoadingConfig() {
    Pair<TableConfig, Schema> tableConfigSchemaPair = fetchTableConfigAndSchema();
    return getIndexLoadingConfig(tableConfigSchemaPair.getLeft(), tableConfigSchemaPair.getRight());
  }

  /**
   * Constructs the index loading config for the table with the given table config and schema.
   */
  IndexLoadingConfig getIndexLoadingConfig(TableConfig tableConfig, Schema schema);

  /**
   * Interface to handle segment state transitions from CONSUMING to DROPPED
   *
   * @param segmentNameStr name of segment for which the state change is being handled
   */
  default void onConsumingToDropped(String segmentNameStr) {
  }

  /**
   * Interface to handle segment state transitions from CONSUMING to ONLINE
   *
   * @param segmentNameStr name of segment for which the state change is being handled
   */
  default void onConsumingToOnline(String segmentNameStr) {
  }

  /**
   * Return list of segment names that are stale along with reason.
   * @param tableConfig Table Config of the table
   * @param schema Schema of the table
   * @return List of {@link StaleSegment} with segment names and reason why it is stale
   */
  List<StaleSegment> getStaleSegments(TableConfig tableConfig, Schema schema);
}
