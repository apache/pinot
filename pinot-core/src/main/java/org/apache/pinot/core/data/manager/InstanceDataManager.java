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
package org.apache.pinot.core.data.manager;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.core.util.SegmentRefreshSemaphore;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The <code>InstanceDataManager</code> class is the instance level data manager, which manages all tables and segments
 * served by the instance.
 */
@ThreadSafe
public interface InstanceDataManager {

  /**
   * Initializes the data manager.
   * <p>Should be called only once and before calling any other method.
   * <p>NOTE: The config is the subset of server config with prefix 'pinot.server.instance'
   */
  void init(PinotConfiguration config, HelixManager helixManager, ServerMetrics serverMetrics)
      throws ConfigurationException;

  /**
   * Returns the instance id.
   */
  String getInstanceId();

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
   * Delete a table.
   */
  void deleteTable(String tableNameWithType)
      throws Exception;

  /**
   * Adds a segment into an REALTIME table.
   * <p>The segment might be committed or under consuming.
   */
  void addRealtimeSegment(String realtimeTableName, String segmentName)
      throws Exception;

  /**
   * Offloads a segment from table but not dropping its data from server.
   */
  void offloadSegment(String tableNameWithType, String segmentName)
      throws Exception;

  /**
   * Delete segment data from the server physically.
   */
  void deleteSegment(String tableNameWithType, String segmentName)
      throws Exception;

  /**
   * Reloads a segment in a table. This method can download a new segment to replace the local
   * one before loading. Download happens when local segment's CRC mismatches the one of
   * the remote segment; but can also be forced to do regardless of CRC.
   */
  void reloadSegment(String tableNameWithType, String segmentName, boolean forceDownload)
      throws Exception;

  /**
   * Reloads all segments of a table.
   * @param segmentRefreshSemaphore semaphore to control concurrent segment reloads/refresh
   */
  void reloadAllSegments(String tableNameWithType, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception;

  /**
   * Reload a list of segments in a table.
   * @param segmentNames is the list of segment to reload
   * @param segmentRefreshSemaphore semaphore to control concurrent segment reloads/refresh
   */
  void reloadSegments(String tableNameWithType, List<String> segmentNames, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception;

  /**
   * Adds or replaces a segment in a table. Different from segment reloading, this method
   * doesn't assume the existence of TableDataManager object and it can actually initialize
   * the TableDataManager for the segment. A new segment is downloaded if the local one is
   * not working or has a different CRC from the remote one.
   */
  void addOrReplaceSegment(String tableNameWithType, String segmentName)
      throws Exception;

  /**
   * Returns all tables served by the instance.
   */
  Set<String> getAllTables();

  /**
   * Returns the table data manager for the given table, or <code>null</code> if it does not exist.
   */
  @Nullable
  TableDataManager getTableDataManager(String tableNameWithType);

  /**
   * Returns the segment metadata for the given segment in the given table, or <code>null</code> if it does not exist.
   */
  @Nullable
  SegmentMetadata getSegmentMetadata(String tableNameWithType, String segmentName);

  /**
   * Returns the metadata for all segments in the given table.
   */
  List<SegmentMetadata> getAllSegmentsMetadata(String tableNameWithType);

  /**
   * Returns the directory for un-tarred segment data.
   */
  File getSegmentDataDirectory(String tableNameWithType, String segmentName);

  /**
   * Returns the directory for tarred segment files.
   */
  String getSegmentFileDirectory();

  /**
   * Returns the maximum number of segments allowed to refresh in parallel.
   */
  int getMaxParallelRefreshThreads();

  /**
   * Returns the Helix property store.
   */
  ZkHelixPropertyStore<ZNRecord> getPropertyStore();

  /**
   * Returns the segment uploader, which uploads a llc segment to the destination place and returns the url of
   * uploaded segment file. Servers utilize segment uploader to upload llc segment to segment store.
   */
  SegmentUploader getSegmentUploader();

  /**
   * Immediately stop consumption and start committing the consuming segments.
   */
  void forceCommit(String tableNameWithType, Set<String> segmentNames);

  /**
   * Enables the installation of a method to determine if a server is ready to server queries.
   *
   * @param isServerReadyToServeQueries supplier to retrieve state of server.
   */
  void setSupplierOfIsServerReadyToServeQueries(Supplier<Boolean> isServerReadyToServeQueries);
}
