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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.TableSnapshot;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Table data manager for OFFLINE table.
@ThreadSafe
public class OfflineTableDataManager extends BaseTableDataManager {

  /// Native conditional load/reload. Local installation receipts still require consensus acceptance.
  public NativeOfflineSegmentAction.Executor nativeExecutor() {
    return nativeActionExecutor();
  }

  /// Initializes an ordinary OFFLINE table using coherent metadata snapshots, without a Helix manager or store.
  /// Extensions require their own native metadata adapter before they can use this initializer.
  public void initWithMetadata(InstanceDataManagerConfig instanceDataManagerConfig, TableDataManagerMetadata metadata,
      SegmentLocks segmentLocks, String tableNameWithType, SegmentReloadSemaphore segmentReloadSemaphore,
      ExecutorService segmentReloadRefreshExecutor, @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable Cache<Pair<String, String>, SegmentErrorInfo> errorCache,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet, boolean enableAsyncSegmentRefresh,
      ServerReloadJobStatusCache reloadJobStatusCache) {
    Preconditions.checkArgument(getClass() == OfflineTableDataManager.class,
        "Native metadata does not support table data manager extensions");
    TableSnapshot snapshot = Objects.requireNonNull(metadata, "metadata").fetchTable(tableNameWithType);
    Preconditions.checkArgument(tableNameWithType.equals(snapshot.tableConfig().getTableName()),
        "Native table identity differs: %s", tableNameWithType);
    initWithMetadataInternal(instanceDataManagerConfig, metadata, segmentLocks, snapshot, segmentReloadSemaphore,
        segmentReloadRefreshExecutor, segmentPreloadExecutor, errorCache, segmentOperationsThrottlerSet,
        enableAsyncSegmentRefresh, reloadJobStatusCache);
  }

  @Override
  protected void doInit() {
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    TableConfig tableConfig = tableConfigAndSchema.getLeft();
    Schema schema = tableConfigAndSchema.getRight();
    if (tableConfig.isUpsertEnabled()) {
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_instanceDataManagerConfig.getUpsertConfig(), tableConfig, schema,
              this, _segmentOperationsThrottlerSet);
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    if (_tableUpsertMetadataManager != null) {
      _tableUpsertMetadataManager.stop();
    }
    releaseAndRemoveAllSegments();
    if (_tableUpsertMetadataManager != null) {
      try {
        _tableUpsertMetadataManager.close();
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
    }
  }

  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    IndexLoadingConfig indexLoadingConfig = getCachedIndexLoadingConfig().withSegmentTier(zkMetadata.getTier());
    handleUpsertPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    } else {
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }
  }

  @Override
  protected void doAddSegment(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment, zkMetadata);
      return;
    }
    super.doAddSegment(immutableSegment, zkMetadata);
  }

  @Override
  public void addConsumingSegment(String segmentName) {
    throw new UnsupportedOperationException("Cannot add CONSUMING segment to OFFLINE table");
  }
}
