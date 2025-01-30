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
package org.apache.pinot.core.data.manager.provider;

import com.google.common.cache.Cache;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Factory for {@link TableDataManager}.
 */
@InterfaceAudience.Private
public interface TableDataManagerProvider {

  void init(InstanceDataManagerConfig instanceDataManagerConfig, HelixManager helixManager, SegmentLocks segmentLocks,
      @Nullable SegmentPreprocessThrottler segmentPreprocessThrottler,
      @Nullable SegmentStarTreePreprocessThrottler segmentStarTreePreprocessThrottler);

  default TableDataManager getTableDataManager(TableConfig tableConfig) {
    return getTableDataManager(tableConfig, null, null, () -> true);
  }

  TableDataManager getTableDataManager(TableConfig tableConfig, @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable Cache<Pair<String, String>, SegmentErrorInfo> errorCache,
      Supplier<Boolean> isServerReadyToServeQueries);
}
