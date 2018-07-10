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
package com.linkedin.pinot.controller.helix.core.realtime;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Cache for table config.
 */
public class TableConfigCache {

  // TODO: Make cache size, timeout configurable through controller config.
  private static final long DEFAULT_CACHE_SIZE = 50;
  private static final long DEFAULT_CACHE_TIMEOUT_IN_MINUTE = 60;

  private final LoadingCache<String, TableConfig> _tableConfigCache;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public TableConfigCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableConfigCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .expireAfterWrite(DEFAULT_CACHE_TIMEOUT_IN_MINUTE, TimeUnit.MINUTES)
        .build(new CacheLoader<String, TableConfig>() {
          @Override
          public TableConfig load(String tableNameWithType) throws Exception {
            return ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
          }
        });
    _propertyStore = propertyStore;
  }

  public TableConfig getTableConfig(String tableNameWithType) throws ExecutionException {
    return _tableConfigCache.get(tableNameWithType);
  }
}
