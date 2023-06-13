package org.apache.pinot.segment.local.data.manager;

import com.google.common.cache.LoadingCache;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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
public class ReadOnlyTableDataManager implements TableDataManager {
  // underlying table data manager
  private final TableDataManager _tableDataManager;
  private final String readOnlyReason;

  public ReadOnlyTableDataManager(TableDataManager tableDataManager, String readOnlyReason) {
    this._tableDataManager = tableDataManager;
    this.readOnlyReason = readOnlyReason;
  }

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache,
      TableDataManagerParams tableDataManagerParams) {
    _tableDataManager.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics, helixManager,
        segmentPreloadExecutor, errorCache, tableDataManagerParams);
  }

  @Override
  public void start() {
    _tableDataManager.start();
  }

  @Override
  public void shutDown() {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "shutDown");
  }

  @Override
  public boolean isShutDown() {
    return _tableDataManager.isShutDown();
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "addSegment");
  }

  @Override
  public void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "addSegment");
  }

  @Override
  public void addSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata)
      throws Exception {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "addSegment");
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "reloadSegment");
  }

  @Override
  public void addOrReplaceSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata localMetadata)
      throws Exception {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "addOrReplaceSegment");
  }

  @Override
  public void removeSegment(String segmentName) {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "removeSegment");
  }

  @Override
  public boolean tryLoadExistingSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata) {
    throw new ReadOnlyTableDataManagerException(readOnlyReason, "tryLoadExistingSegment");
  }

  @Override
  public File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig) {
    return _tableDataManager.getSegmentDataDir(segmentTier, segmentTier, tableConfig);
  }

  @Override
  public boolean isSegmentDeletedRecently(String segmentName) {
    return _tableDataManager.isSegmentDeletedRecently(segmentName);
  }

  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    return _tableDataManager.acquireAllSegments();
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments) {
    return _tableDataManager.acquireSegments(segmentNames, missingSegments);
  }

  @Nullable
  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    return _tableDataManager.acquireSegment(segmentName);
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {
    _tableDataManager.releaseSegment(segmentDataManager);
  }

  @Override
  public int getNumSegments() {
    return _tableDataManager.getNumSegments();
  }

  @Override
  public String getTableName() {
    return _tableDataManager.getTableName();
  }

  @Override
  public File getTableDataDir() {
    return _tableDataManager.getTableDataDir();
  }

  @Override
  public TableDataManagerConfig getTableDataManagerConfig() {
    return _tableDataManager.getTableDataManagerConfig();
  }

  @Override
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {
    _tableDataManager.addSegmentError(segmentName, segmentErrorInfo);
  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    return _tableDataManager.getSegmentErrors();
  }

  @Override
  public long getLoadTimeMs() {
    return _tableDataManager.getLoadTimeMs();
  }
}
