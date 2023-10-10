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
package org.apache.pinot.segment.local.data.manager;

import com.google.common.cache.LoadingCache;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * A read-only wrapper for an existing table data manager
 */
public class TableDataManagerDelegate implements TableDataManager {
  // underlying table data manager
  private final Supplier<TableDataManager> _getTableDataManager;

  private TableDataManager getTableDataManager() {
    return _getTableDataManager.get();
  }

  public TableDataManagerDelegate(Supplier<TableDataManager> getTableDataManager) {
    _getTableDataManager = getTableDataManager;
  }

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache,
      TableDataManagerParams tableDataManagerParams) {
    getTableDataManager().init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics, helixManager,
        segmentPreloadExecutor, errorCache, tableDataManagerParams);
  }

  @Override
  public void start() {
    getTableDataManager().start();
  }

  @Override
  public void shutDown() {
    getTableDataManager().shutDown();
  }

  @Override
  public boolean isShutDown() {
    return getTableDataManager().isShutDown();
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    getTableDataManager().addSegment(immutableSegment);
  }

  @Override
  public void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    getTableDataManager().addSegment(indexDir, indexLoadingConfig);
  }

  @Override
  public void addSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata)
      throws Exception {
    getTableDataManager().addSegment(segmentName, indexLoadingConfig, zkMetadata);
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    getTableDataManager().reloadSegment(segmentName, indexLoadingConfig, zkMetadata,
        localMetadata, schema, forceDownload);
  }

  @Override
  public void addOrReplaceSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata localMetadata)
      throws Exception {
    getTableDataManager().addOrReplaceSegment(segmentName, indexLoadingConfig, zkMetadata, localMetadata);
  }

  @Override
  public void removeSegment(String segmentName) {
    getTableDataManager().removeSegment(segmentName);
  }

  @Override
  public boolean tryLoadExistingSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata) {
    return getTableDataManager().tryLoadExistingSegment(segmentName, indexLoadingConfig, zkMetadata);
  }

  @Override
  public File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig) {
    return getTableDataManager().getSegmentDataDir(segmentTier, segmentTier, tableConfig);
  }

  @Override
  public boolean isSegmentDeletedRecently(String segmentName) {
    return getTableDataManager().isSegmentDeletedRecently(segmentName);
  }

  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    return getTableDataManager().acquireAllSegments();
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments) {
    return getTableDataManager().acquireSegments(segmentNames, missingSegments);
  }

  @Nullable
  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    return getTableDataManager().acquireSegment(segmentName);
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {
    getTableDataManager().releaseSegment(segmentDataManager);
  }

  @Override
  public int getNumSegments() {
    return getTableDataManager().getNumSegments();
  }

  @Override
  public String getTableName() {
    return getTableDataManager().getTableName();
  }

  @Override
  public File getTableDataDir() {
    return getTableDataManager().getTableDataDir();
  }

  @Override
  public TableDataManagerConfig getTableDataManagerConfig() {
    return getTableDataManager().getTableDataManagerConfig();
  }

  @Override
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {
    getTableDataManager().addSegmentError(segmentName, segmentErrorInfo);
  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    return getTableDataManager().getSegmentErrors();
  }

  @Override
  public long getLoadTimeMs() {
    return getTableDataManager().getLoadTimeMs();
  }
}
