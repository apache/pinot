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
package org.apache.pinot.core.data.manager.callback.impl;

import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.callback.DataManagerCallback;
import org.apache.pinot.core.data.manager.callback.TableDataManagerCallback;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

/**
 * class that used for regular append-only ingestion mode or data processing that don't support upsert
 * all method are no-op to ensure that regular append-only ingestion mode has the same performance as before
 */
public class DefaultTableDataManagerCallbackImpl implements TableDataManagerCallback {

  private static final DefaultDataManagerCallbackImpl DEFAULT_DM_CALLBACK = DefaultDataManagerCallbackImpl.INSTANCE;

  @Override
  public void init() {
  }

  @Override
  public void onSegmentAdd(String tableName, String segmentName) {
  }

  /**
   * return the default cached instance of data manager callback to ensure better performance
   */
  @Override
  public DataManagerCallback getMutableDataManagerCallback(String tableName, String segmentName,
      Schema schema, TableConfig tableConfig, ServerMetrics serverMetrics) {
    return DEFAULT_DM_CALLBACK;
  }

  @Override
  public DataManagerCallback getImmutableDataManagerCallback(String tableName, String segmentName,
      Schema schema, TableConfig tableConfig, ServerMetrics serverMetrics) {
    return DEFAULT_DM_CALLBACK;
  }
  @Override
  public DataManagerCallback getDefaultDataManagerCallback() {
    return DEFAULT_DM_CALLBACK;
  }


}
