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

import com.google.common.cache.LoadingCache;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/**
 * Factory for {@link TableDataManager}.
 */
public class TableDataManagerProvider {
  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final Semaphore _segmentBuildSemaphore;

  public TableDataManagerProvider(InstanceDataManagerConfig instanceDataManagerConfig) {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    int maxParallelSegmentBuilds = instanceDataManagerConfig.getMaxParallelSegmentBuilds();
    _segmentBuildSemaphore = maxParallelSegmentBuilds > 0 ? new Semaphore(maxParallelSegmentBuilds, true) : null;
  }

  public TableDataManager getTableDataManager(TableConfig tableConfig, HelixManager helixManager) {
    return getTableDataManager(tableConfig, helixManager, null, null, () -> true);
  }

  public TableDataManager getTableDataManager(TableConfig tableConfig, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache,
      Supplier<Boolean> isServerReadyToServeQueries) {
    TableDataManager tableDataManager;
    switch (tableConfig.getTableType()) {
      case OFFLINE:
        if (tableConfig.isDimTable()) {
          tableDataManager = DimensionTableDataManager.createInstanceByTableName(tableConfig.getTableName());
        } else {
          tableDataManager = new OfflineTableDataManager();
        }
        break;
      case REALTIME:
        Map<String, String> streamConfigMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
        if (Boolean.parseBoolean(streamConfigMap.get(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE))
            && StringUtils.isEmpty(_instanceDataManagerConfig.getSegmentStoreUri())) {
          throw new IllegalStateException(String.format("Table has enabled %s config. But the server has not "
                  + "configured the segmentstore uri. Configure the server config %s",
              StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE, CommonConstants.Server.CONFIG_OF_SEGMENT_STORE_URI));
        }
        tableDataManager = new RealtimeTableDataManager(_segmentBuildSemaphore, isServerReadyToServeQueries);
        break;
      default:
        throw new IllegalStateException();
    }
    tableDataManager.init(_instanceDataManagerConfig, tableConfig, helixManager, segmentPreloadExecutor, errorCache);
    return tableDataManager;
  }
}
