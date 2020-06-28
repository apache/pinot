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
package org.apache.pinot.core.data.manager.callback;

import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

import java.io.File;

/**
 * Component inject to {@link org.apache.pinot.core.data.manager.TableDataManager} for handling extra logic for
 * other workflows other than regular append-mode ingestion.
 */
@InterfaceStability.Evolving
public interface TableDataManagerCallback {

  /**
   * Initialize callback from {@link org.apache.pinot.core.data.manager.TableDataManager}, injected into
   * {@link org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager} init() method to make sure this class
   * is initialized during the creation process.
   */
  void init();

  /**
   * Handle any internal logic for upsert table to add segment to its internal update log storage. this method is injected into
   * RealtimeTableDataManager#addSegment(File, IndexLoadingConfig)
   *
   * In upsert-enabled tables callback, this method will notify the local update log storage to create proper storage provider
   * for this new segment
   *
   * @param tableName the name of the table we are trying to add the segment to
   * @param segmentName the name of the segment we are trying to add
   */
  void onSegmentAdd(String tableName, String segmentName);

  /**
   * Return a callback object for a mutable segment data manager callback component when a table create a new
   * mutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}. We will create a proper callback based on
   * whether the current pinot server config and whether the table is upsert-enabled
   *
   * @param tableName the name of the table
   * @param segmentName the name of the segment
   * @param schema the table schema
   * @param tableConfig the config of the table
   * @param serverMetrics the server metrics object
   * @return {@link DataManagerCallback} object appropriate for this table, either a regular append-only callback
   * or a upsert-enabled callback
   */
  DataManagerCallback getMutableDataManagerCallback(String tableName, String segmentName, Schema schema,
      TableConfig tableConfig, ServerMetrics serverMetrics);

  /**
   * Return a callback object for an Immutable segment data manager callback component when a table create a new
   * immutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}, we will create a proper callback based on
   * whether the current pinot server config and whether the table is upsert-enabled
   *
   * @param tableName the name of the table
   * @param segmentName the name of the segment
   * @param schema the table schema
   * @param tableConfig the config of the table
   * @param serverMetrics the server metrics object
   * @return {@link DataManagerCallback} object appropriate for this table, either a regular append-only callback
   * or a upsert-enabled callback
   */
  DataManagerCallback getImmutableDataManagerCallback(String tableName, String segmentName, Schema schema,
      TableConfig tableConfig, ServerMetrics serverMetrics);

  /**
   * Create a no-op default callback for segmentDataManager that don't support upsert
   * (eg, offline table, HLL consumers etc)
   */
  DataManagerCallback getDefaultDataManagerCallback();
}
