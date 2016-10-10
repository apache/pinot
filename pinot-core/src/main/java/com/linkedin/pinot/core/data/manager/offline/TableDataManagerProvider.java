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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeTableDataManager;


/**
 * Provide static function to get PartitionDataManager implementation.
 *
 */
public class TableDataManagerProvider {
  private static ServerMetrics SERVER_METRICS;

  private static Map<String, Class<? extends TableDataManager>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends TableDataManager>>();

  static {
    keyToFunction.put("offline", OfflineTableDataManager.class);
    keyToFunction.put("realtime", RealtimeTableDataManager.class);
  }

  public static TableDataManager getTableDataManager(TableDataManagerConfig tableDataManagerConfig,
      String serverInstance) {
    Preconditions.checkNotNull(SERVER_METRICS);

    try {
      Class<? extends TableDataManager> cls =
          keyToFunction.get(tableDataManagerConfig.getTableDataManagerType().toLowerCase());
      if (cls != null) {
        TableDataManager tableDataManager = cls.newInstance();
        tableDataManager.init(tableDataManagerConfig, SERVER_METRICS, serverInstance);
        return tableDataManager;
      } else {
        throw new UnsupportedOperationException("No tableDataManager type found.");
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support tableDataManager type with - "
          + tableDataManagerConfig.getTableDataManagerType(), ex);
    }
  }

  public static void setServerMetrics(ServerMetrics serverMetrics) {
    SERVER_METRICS = serverMetrics;
  }

  public static ServerMetrics getServerMetrics() {
    return SERVER_METRICS;
  }
}
