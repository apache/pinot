/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Factory for {@link TableDataManager}.
 */
public class TableDataManagerProvider {
  private TableDataManagerProvider() {
  }

  private static final String OFFLINE_TABLE_DATA_MANAGER_TYPE = "OFFLINE";
  private static final String REALTIME_TABLE_DATA_MANAGER_TYPE = "REALTIME";

  public static TableDataManager getTableDataManager(@Nonnull TableDataManagerConfig tableDataManagerConfig,
      @Nonnull String instanceId, @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull ServerMetrics serverMetrics) {
    String tableDataManagerType = tableDataManagerConfig.getTableDataManagerType().toUpperCase();
    TableDataManager tableDataManager;
    switch (tableDataManagerType) {
      case OFFLINE_TABLE_DATA_MANAGER_TYPE:
        tableDataManager = new OfflineTableDataManager();
        break;
      case REALTIME_TABLE_DATA_MANAGER_TYPE:
        tableDataManager = new RealtimeTableDataManager();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported table data manager type: " + tableDataManagerType + " for table: "
                + tableDataManagerConfig.getTableName());
    }
    tableDataManager.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics);
    return tableDataManager;
  }
}
