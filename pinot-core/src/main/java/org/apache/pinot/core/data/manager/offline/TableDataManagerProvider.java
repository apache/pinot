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
package org.apache.pinot.core.data.manager.offline;

import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableType;


/**
 * Factory for {@link TableDataManager}.
 */
public class TableDataManagerProvider {
  private static Semaphore _segmentBuildSemaphore;

  private TableDataManagerProvider() {
  }

  public static void init(InstanceDataManagerConfig instanceDataManagerConfig) {
    int maxParallelBuilds = instanceDataManagerConfig.getMaxParallelSegmentBuilds();
    if (maxParallelBuilds > 0) {
      _segmentBuildSemaphore = new Semaphore(maxParallelBuilds, true);
    }
  }

  public static TableDataManager getTableDataManager(@Nonnull TableDataManagerConfig tableDataManagerConfig,
      @Nonnull String instanceId, @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull ServerMetrics serverMetrics, @Nonnull HelixManager helixManager) {
    TableDataManager tableDataManager;
    switch (TableType.valueOf(tableDataManagerConfig.getTableDataManagerType())) {
      case OFFLINE:
        if (tableDataManagerConfig.isDimTable()) {
          tableDataManager =
              DimensionTableDataManager.createInstanceByTableName(tableDataManagerConfig.getTableName());
        } else {
          tableDataManager = new OfflineTableDataManager();
        }
        break;
      case REALTIME:
        tableDataManager = new RealtimeTableDataManager(_segmentBuildSemaphore);
        break;
      default:
        throw new IllegalStateException();
    }
    tableDataManager.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics, helixManager);
    return tableDataManager;
  }
}
