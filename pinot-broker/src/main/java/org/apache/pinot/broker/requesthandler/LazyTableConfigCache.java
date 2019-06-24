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
package org.apache.pinot.broker.requesthandler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;


/**
 * A cache for table-config objects maintained by the broker. The cache is implemented as a lazy-cache: i.e., on a
 * cache miss, the value is fetched asynchronously/lazily - the caller is not blocked and can expect to get null or
 * a previously cached value.
 *
 * This cache should be used only for best-effort cases, where absence of the value does not have critical implications.
 *
 * The cache is unbounded as we don't expect an ever-growing or very large number of tables to be served by a single
 * broker.
 */
@ThreadSafe
public class LazyTableConfigCache {

  private static final int CACHE_TIMEOUT_MINS = 60;

  private final LoadingCache<String, TableConfig> _configCache;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ExecutorService _executorService;

  public LazyTableConfigCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    _executorService = Executors.newCachedThreadPool();

    _configCache = CacheBuilder.newBuilder().refreshAfterWrite(CACHE_TIMEOUT_MINS, TimeUnit.MINUTES)
        .build(new CacheLoader<String, TableConfig>() {
          @Override
          public TableConfig load(@Nonnull String rawTableName) {
            return ZKMetadataProvider.getTableConfig(_propertyStore, rawTableName);
          }

          @Override
          public ListenableFuture<TableConfig> reload(String tableName, TableConfig oldValue) {
            ListenableFutureTask<TableConfig> task =
                ListenableFutureTask.create(() -> ZKMetadataProvider.getTableConfig(_propertyStore, tableName));
            _executorService.execute(task);
            return task;
          }
        });
  }

  /**
   * Returns the table-config if present - kicks off a lazy load otherwise.
   */
  @Nullable
  public TableConfig get(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    TableConfig config = _configCache.getIfPresent(rawTableName);
    if (config == null) {
      // load the value asynchronously
      ListenableFutureTask<TableConfig> task =
          ListenableFutureTask.create(() -> _configCache.get(rawTableName));
      _executorService.execute(task);
    }

    return config;
  }

  /**
   * Shut the cache down and any running tasks/threads.
   */
  public void shutDown() {
    _executorService.shutdown();
  }
}


