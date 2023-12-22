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
package org.apache.pinot.segment.local.upsert;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * The manager of the upsert metadata of a table.
 */
@ThreadSafe
public interface TableUpsertMetadataManager extends Closeable {

  void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor);

  PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId);

  PartitionUpsertMetadataManager get(int partitionId);

  UpsertConfig.Mode getUpsertMode();

  /**
   * Stops the metadata manager. After invoking this method, no access to the metadata will be accepted.
   */
  void stop();

  boolean isPreloading();
}
