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
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

import java.io.File;

/**
 * component inject to {@link org.apache.pinot.core.data.manager.TableDataManager} for handling extra logic for
 * other workflows other than regular append-mode ingestion.
 */
public interface TableDataManagerCallback {

  void init();

  /**
   * Callback to ensure other components related to the callback are added when
   * {@link org.apache.pinot.core.data.manager.TableDataManager#addSegment(File, IndexLoadingConfig)} is called
   */
  void addSegment(String tableName, String segmentName);

  /**
   * return a callback object for a mutable segment data manager callback component when a table create a new
   * immutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}
   */
  DataManagerCallback getMutableDataManagerCallback(String tableName, String segmentName, Schema schema,
      TableConfig tableConfig, ServerMetrics serverMetrics);

  /**
   * return a callback object for an Immutable segment data manager callback component when a table create a new
   * immutable {@link org.apache.pinot.core.data.manager.SegmentDataManager}
   */
  DataManagerCallback getImmutableDataManagerCallback(String tableName, String segmentName, Schema schema,
      TableConfig tableConfig, ServerMetrics serverMetrics);

  /**
   * create a no-op default callback for segmentDataManager that don't support upsert
   * (eg, offline table, HLL consumers etc)
   * @return a no-op default callback for data manager
   */
  DataManagerCallback getDefaultDataManagerCallback();
}
