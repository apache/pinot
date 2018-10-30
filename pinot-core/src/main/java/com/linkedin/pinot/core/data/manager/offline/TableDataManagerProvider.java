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
package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.data.manager.TableDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


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
      @Nonnull ServerMetrics serverMetrics) {
    TableDataManager tableDataManager;
    switch (CommonConstants.Helix.TableType.valueOf(tableDataManagerConfig.getTableDataManagerType())) {
      case OFFLINE:
        tableDataManager = new OfflineTableDataManager();
        break;
      case REALTIME:
        tableDataManager = new RealtimeTableDataManager(_segmentBuildSemaphore);
        break;
      default:
        throw new IllegalStateException();
    }
    tableDataManager.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics);
    return tableDataManager;
  }
}
