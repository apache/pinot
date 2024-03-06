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
package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BasePartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  protected static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

  protected final String _tableNameWithType;
  protected final int _partitionId;
  protected final UpsertContext _context;
  protected final List<String> _primaryKeyColumns;
  protected final List<String> _comparisonColumns;
  protected final String _deleteRecordColumn;
  protected final HashFunction _hashFunction;
  protected final PartialUpsertHandler _partialUpsertHandler;
  protected final boolean _enableSnapshot;
  protected final double _metadataTTL;
  protected final double _deletedKeysTTL;
  protected final File _tableIndexDir;
  protected final ServerMetrics _serverMetrics;
  protected final Logger _logger;

  // Tracks all the segments managed by this manager (excluding EmptySegment)
  protected final Set<IndexSegment> _trackedSegments = ConcurrentHashMap.newKeySet();

  // NOTE: We do not persist snapshot on the first consuming segment because most segments might not be loaded yet
  protected volatile boolean _gotFirstConsumingSegment = false;
  protected final ReadWriteLock _snapshotLock;

  protected long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  protected int _numOutOfOrderEvents = 0;

  // Used to maintain the largestSeenComparisonValue to avoid handling out-of-ttl segments/records.
  // If upsertTTL enabled, we will keep track of largestSeenComparisonValue to compute expired segments.
  protected final AtomicDouble _largestSeenComparisonValue;

  // The following variables are always accessed within synchronized block
  private boolean _stopped;
  // Initialize with 1 pending operation to indicate the metadata manager can take more operations
  private int _numPendingOperations = 1;
  private boolean _closed;
  // The lock and boolean flag ensure only one thread can start preloading and preloading happens only once.
  private final Lock _preloadLock = new ReentrantLock();
  private volatile boolean _isPreloading;

  protected BasePartitionUpsertMetadataManager(String tableNameWithType, int partitionId, UpsertContext context) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _context = context;
    _primaryKeyColumns = context.getPrimaryKeyColumns();
    _comparisonColumns = context.getComparisonColumns();
    _deleteRecordColumn = context.getDeleteRecordColumn();
    _hashFunction = context.getHashFunction();
    _partialUpsertHandler = context.getPartialUpsertHandler();
    _enableSnapshot = context.isSnapshotEnabled();
    _snapshotLock = _enableSnapshot ? new ReentrantReadWriteLock() : null;
    _isPreloading = _enableSnapshot && context.isPreloadEnabled();
    _metadataTTL = context.getMetadataTTL();
    _deletedKeysTTL = context.getDeletedKeysTTL();
    _tableIndexDir = context.getTableIndexDir();
    _serverMetrics = ServerMetrics.get();
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
    if (_metadataTTL > 0) {
      _largestSeenComparisonValue = new AtomicDouble(loadWatermark());
    } else {
      _largestSeenComparisonValue = new AtomicDouble(Double.MIN_VALUE);
      deleteWatermark();
    }
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Nullable
  protected MutableRoaringBitmap getQueryableDocIds(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    if (_deleteRecordColumn == null) {
      return null;
    }
    MutableRoaringBitmap queryableDocIds = new MutableRoaringBitmap();
    try (PinotSegmentColumnReader deleteRecordColumnReader = new PinotSegmentColumnReader(segment,
        _deleteRecordColumn)) {
      PeekableIntIterator docIdIterator = validDocIds.getIntIterator();
      while (docIdIterator.hasNext()) {
        int docId = docIdIterator.next();
        if (!BooleanUtils.toBoolean(deleteRecordColumnReader.getValue(docId))) {
          queryableDocIds.add(docId);
        }
      }
    } catch (IOException e) {
      _logger.error("Failed to close column reader for delete record column: {} for segment: {} ", _deleteRecordColumn,
          segment.getSegmentName(), e);
    }
    return queryableDocIds;
  }

  @Override
  public boolean isPreloading() {
    return _isPreloading;
  }

  @Override
  public void preloadSegments(IndexLoadingConfig indexLoadingConfig) {
    if (!_isPreloading) {
      return;
    }
    TableDataManager tableDataManager = _context.getTableDataManager();
    Preconditions.checkNotNull(tableDataManager, "Preloading segments requires tableDataManager");
    HelixManager helixManager = tableDataManager.getHelixManager();
    ExecutorService segmentPreloadExecutor = tableDataManager.getSegmentPreloadExecutor();
    // Preloading the segments with the snapshots of validDocIds for fast upsert metadata recovery.
    // Note that there is a waiting logic between the thread pool doing the segment preloading here and the
    // other helix threads about to process segment state transitions (e.g. taking segments from OFFLINE to ONLINE).
    // The thread doing the segment preloading here must complete before the other helix threads start to handle
    // segment state transitions. This is ensured by the lock here.
    _preloadLock.lock();
    try {
      // Check the flag again to ensure preloading happens only once.
      if (!_isPreloading) {
        return;
      }
      // From now on, the _isPreloading flag is true until the segments are preloaded.
      doPreloadSegments(tableDataManager, indexLoadingConfig, helixManager, segmentPreloadExecutor);
    } catch (Exception e) {
      // Even if preloading fails, we should continue to complete the initialization, so that TableDataManager can be
      // created. Once TableDataManager is created, no more segment preloading would happen, and the normal segment
      // loading logic would be used. The segments not being preloaded successfully here would be loaded via the
      // normal segment loading logic, the one doing more costly checks on the upsert metadata.
      _logger.warn("Failed to preload segments from partition: {} of table: {}, skipping", _partitionId,
          _tableNameWithType, e);
      if (e instanceof InterruptedException) {
        // Restore the interrupted status in case the upper callers want to check.
        Thread.currentThread().interrupt();
      }
    } finally {
      _isPreloading = false;
      _preloadLock.unlock();
    }
  }

  protected void doPreloadSegments(TableDataManager tableDataManager, IndexLoadingConfig indexLoadingConfig,
      HelixManager helixManager, ExecutorService segmentPreloadExecutor)
      throws Exception {
    _logger.info("Preload segments from partition: {} of table: {} for fast upsert metadata recovery", _partitionId,
        _tableNameWithType);
    String instanceId = getInstanceId(tableDataManager);
    Map<String, Map<String, String>> segmentAssignment = getSegmentAssignment(helixManager);
    Map<String, SegmentZKMetadata> segmentMetadataMap = getSegmentsZKMetadata(helixManager);
    List<Future<?>> futures = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String state = instanceStateMap.get(instanceId);
      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
        if (state == null) {
          _logger.debug("Skip segment: {} as it's not assigned to instance: {}", segmentName, instanceId);
        } else {
          _logger.info("Skip segment: {} as its ideal state: {} is not ONLINE for instance: {}", segmentName, state,
              instanceId);
        }
        continue;
      }
      SegmentZKMetadata segmentZKMetadata = segmentMetadataMap.get(segmentName);
      Preconditions.checkState(segmentZKMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s",
          segmentName, _tableNameWithType);
      Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, segmentZKMetadata, null);
      Preconditions.checkNotNull(partitionId,
          String.format("Failed to get partition id for segment: %s (upsert-enabled table: %s)", segmentName,
              _tableNameWithType));
      if (partitionId != _partitionId) {
        _logger.debug("Skip segment: {} as its partition: {} is different from the requested partition: {}",
            segmentName, partitionId, _partitionId);
        continue;
      }
      if (!hasValidDocIdsSnapshot(tableDataManager, indexLoadingConfig.getTableConfig(), segmentName,
          segmentZKMetadata.getTier())) {
        _logger.info("Skip segment: {} from partition: {} as no validDocIds snapshot exists", segmentName,
            _partitionId);
        continue;
      }
      futures.add(segmentPreloadExecutor.submit(
          () -> doPreloadSegmentWithSnapshot(tableDataManager, segmentName, indexLoadingConfig, segmentZKMetadata)));
    }
    try {
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      for (Future<?> f : futures) {
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
    }
    _logger.info("Preloaded segments from partition: {} of table: {} for fast upsert metadata recovery", _partitionId,
        _tableNameWithType);
  }

  private String getInstanceId(TableDataManager tableDataManager) {
    return tableDataManager.getInstanceDataManagerConfig().getInstanceId();
  }

  private static boolean hasValidDocIdsSnapshot(TableDataManager tableDataManager, TableConfig tableConfig,
      String segmentName, String segmentTier) {
    try {
      File indexDir = tableDataManager.getSegmentDataDir(segmentName, segmentTier, tableConfig);
      File snapshotFile =
          new File(SegmentDirectoryPaths.findSegmentDirectory(indexDir), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
      return snapshotFile.exists();
    } catch (Exception e) {
      return false;
    }
  }

  @VisibleForTesting
  Map<String, Map<String, String>> getSegmentAssignment(HelixManager helixManager) {
    IdealState idealState = HelixHelper.getTableIdealState(helixManager, _tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", _tableNameWithType);
    return idealState.getRecord().getMapFields();
  }

  @VisibleForTesting
  Map<String, SegmentZKMetadata> getSegmentsZKMetadata(HelixManager helixManager) {
    Map<String, SegmentZKMetadata> segmentMetadataMap = new HashMap<>();
    ZKMetadataProvider.getSegmentsZKMetadata(helixManager.getHelixPropertyStore(), _tableNameWithType)
        .forEach(m -> segmentMetadataMap.put(m.getSegmentName(), m));
    return segmentMetadataMap;
  }

  @VisibleForTesting
  void doPreloadSegmentWithSnapshot(TableDataManager tableDataManager, String segmentName,
      IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata segmentZKMetadata) {
    try {
      _logger.info("Preload segment: {} from partition: {} of table: {}", segmentName, _partitionId,
          _tableNameWithType);
      // This method checks segment crc and if it has changed, the segment is not loaded. It might modify the
      // file on disk, but we don't need to take the segmentLock, because every segment from the current table is
      // processed by at most one thread from the preloading thread pool. HelixTaskExecutor task threads about to
      // process segments from the same table are blocked on _preloadLock.
      // In fact, taking segmentLock during segment preloading phase could cause deadlock when HelixTaskExecutor
      // threads processing other tables have taken the same segmentLock as decided by the hash of table name and
      // segment name, i.e. due to hash collision.
      tableDataManager.tryLoadExistingSegment(segmentName, indexLoadingConfig, segmentZKMetadata);
      _logger.info("Preloaded segment: {} from partition: {} of table: {}", segmentName, _partitionId,
          _tableNameWithType);
    } catch (Exception e) {
      _logger.warn("Failed to preload segment: {} from partition: {} of table: {}, skipping", segmentName, _partitionId,
          _tableNameWithType, e);
    }
  }

  @Override
  public void addSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);
    ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) segment;

    if (_deletedKeysTTL > 0) {
      double maxComparisonValue =
          ((Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0))
              .getMaxValue()).doubleValue();
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, maxComparisonValue));
    }

    // Skip adding segment that has max comparison value smaller than (largestSeenComparisonValue - TTL)
    if (_metadataTTL > 0 && _largestSeenComparisonValue.get() > 0) {
      Preconditions.checkState(_enableSnapshot, "Upsert TTL must have snapshot enabled");
      Preconditions.checkState(_comparisonColumns.size() == 1,
          "Upsert TTL does not work with multiple comparison columns");
      Number maxComparisonValue =
          (Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0)).getMaxValue();
      if (maxComparisonValue.doubleValue() < _largestSeenComparisonValue.get() - _metadataTTL) {
        _logger.info("Skip adding segment: {} because it's out of TTL", segmentName);
        MutableRoaringBitmap validDocIdsSnapshot = immutableSegment.loadValidDocIdsFromSnapshot();
        if (validDocIdsSnapshot != null) {
          MutableRoaringBitmap queryableDocIds = getQueryableDocIds(segment, validDocIdsSnapshot);
          immutableSegment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(validDocIdsSnapshot),
              new ThreadSafeMutableRoaringBitmap(queryableDocIds));
        } else {
          _logger.warn("Failed to find snapshot from segment: {} which is out of TTL, treating all documents as valid",
              segmentName);
        }
        return;
      }
    }

    if (!startOperation()) {
      _logger.info("Skip adding segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doAddSegment(immutableSegment);
      _trackedSegments.add(segment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
    }
  }

  protected void doAddSegment(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Adding segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds;
    if (_enableSnapshot) {
      validDocIds = segment.loadValidDocIdsFromSnapshot();
      if (validDocIds != null && validDocIds.isEmpty()) {
        _logger.info("Skip adding segment: {} without valid doc, current primary key count: {}",
            segment.getSegmentName(), getNumPrimaryKeys());
        segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(), null);
        return;
      }
    } else {
      validDocIds = null;
      segment.deleteValidDocIdsSnapshot();
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      Iterator<RecordInfo> recordInfoIterator;
      if (validDocIds != null) {
        recordInfoIterator = UpsertUtils.getRecordInfoIterator(recordInfoReader, validDocIds);
      } else {
        recordInfoIterator =
            UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      }
      addSegment(segment, null, null, recordInfoIterator);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while adding segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    updatePrimaryKeyGauge(numPrimaryKeys);
    _logger.info("Finished adding segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  protected abstract long getNumPrimaryKeys();

  protected void updatePrimaryKeyGauge(long numPrimaryKeys) {
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);
  }

  protected void updatePrimaryKeyGauge() {
    updatePrimaryKeyGauge(getNumPrimaryKeys());
  }

  @Override
  public void preloadSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(_enableSnapshot, "Snapshot must be enabled to preload segment: %s, table: %s",
        segmentName, _tableNameWithType);
    // Note that EmptyIndexSegment should not reach here either, as it doesn't have validDocIds snapshot.
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);
    if (!startOperation()) {
      _logger.info("Skip preloading segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    _snapshotLock.readLock().lock();
    try {
      doPreloadSegment((ImmutableSegmentImpl) segment);
      _trackedSegments.add(segment);
    } finally {
      _snapshotLock.readLock().unlock();
      finishOperation();
    }
  }

  protected void doPreloadSegment(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Preloading segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds = segment.loadValidDocIdsFromSnapshot();
    Preconditions.checkState(validDocIds != null,
        "Snapshot of validDocIds is required to preload segment: %s, table: %s", segmentName, _tableNameWithType);
    if (validDocIds.isEmpty()) {
      _logger.info("Skip preloading segment: {} without valid doc, current primary key count: {}",
          segment.getSegmentName(), getNumPrimaryKeys());
      segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(), null);
      return;
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      doPreloadSegment(segment, null, null, UpsertUtils.getRecordInfoIterator(recordInfoReader, validDocIds));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while preloading segment: %s, table: %s", segmentName, _tableNameWithType),
          e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    updatePrimaryKeyGauge(numPrimaryKeys);
    _logger.info("Finished preloading segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  /**
   * NOTE: no need to get segmentLock to preload segment as callers ensure the segment is processed by a single thread.
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  void doPreloadSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    if (validDocIds == null) {
      validDocIds = new ThreadSafeMutableRoaringBitmap();
    }
    if (queryableDocIds == null && _deleteRecordColumn != null) {
      queryableDocIds = new ThreadSafeMutableRoaringBitmap();
    }
    addSegmentWithoutUpsert(segment, validDocIds, queryableDocIds, recordInfoIterator);
  }

  /**
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  public void addSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      if (queryableDocIds == null && _deleteRecordColumn != null) {
        queryableDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment);

  protected void addSegmentWithoutUpsert(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
  }

  @Override
  public boolean addRecord(MutableSegment segment, RecordInfo recordInfo) {
    _gotFirstConsumingSegment = true;
    if (!startOperation()) {
      _logger.debug("Skip adding record to segment: {} because metadata manager is already stopped",
          segment.getSegmentName());
      return false;
    }
    // NOTE: We don't acquire snapshot read lock here because snapshot is always taken before a new consuming segment
    //       starts consuming, so it won't overlap with this method
    try {
      boolean addRecord = doAddRecord(segment, recordInfo);
      _trackedSegments.add(segment);
      return addRecord;
    } finally {
      finishOperation();
    }
  }

  /**
   * Returns {@code true} when the record is added to the upsert metadata manager, {@code false} when the record is
   * out-of-order thus not added.
   */
  protected abstract boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo);

  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    if (!startOperation()) {
      _logger.info("Skip replacing segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doReplaceSegment(segment, oldSegment);
      if (!(segment instanceof EmptyIndexSegment)) {
        _trackedSegments.add(segment);
      }
      _trackedSegments.remove(oldSegment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
    }
  }

  protected void doReplaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing {} segment: {}, current primary key count: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      replaceSegment(segment, null, null, null, oldSegment);
      return;
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      Iterator<RecordInfo> recordInfoIterator =
          UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      replaceSegment(segment, null, null, recordInfoIterator, oldSegment);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while replacing segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    updatePrimaryKeyGauge(numPrimaryKeys);
    _logger.info("Finished replacing segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  public void replaceSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, @Nullable Iterator<RecordInfo> recordInfoIterator,
      IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);

    // Currently when TTL is enabled, we don't support skip loading out-of-TTL segment with snapshots, since we don't
    // know which docs are valid in the new segment.
    // TODO: when ttl is enabled, we can allow
    //       (1) skip loading segments without any invalid docs.
    //       (2) assign the invalid docs from the replaced segment to the new segment.
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIdsForOldSegment =
          oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (recordInfoIterator != null) {
        Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
            "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
            _tableNameWithType);
        if (validDocIds == null) {
          validDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        if (queryableDocIds == null && _deleteRecordColumn != null) {
          queryableDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, queryableDocIds, recordInfoIterator,
            oldSegment, validDocIdsForOldSegment);
      }

      if (validDocIdsForOldSegment != null && !validDocIdsForOldSegment.isEmpty()) {
        int numKeysNotReplaced = validDocIdsForOldSegment.getCardinality();
        if (_partialUpsertHandler != null) {
          // For partial-upsert table, because we do not restore the original record location when removing the primary
          // keys not replaced, it can potentially cause inconsistency between replicas. This can happen when a
          // consuming segment is replaced by a committed segment that is consumed from a different server with
          // different records (some stream consumer cannot guarantee consuming the messages in the same order).
          _logger.warn("Found {} primary keys not replaced when replacing segment: {} for partial-upsert table. This "
              + "can potentially cause inconsistency between replicas", numKeysNotReplaced, segmentName);
          _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED,
              numKeysNotReplaced);
        } else {
          _logger.info("Found {} primary keys not replaced when replacing segment: {}", numKeysNotReplaced,
              segmentName);
        }
        removeSegment(oldSegment, validDocIdsForOldSegment);
      }
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds);

  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    if (!_trackedSegments.contains(segment)) {
      _logger.info("Skip removing untracked (replaced or empty) segment: {}", segmentName);
      return;
    }
    // Skip removing the upsert metadata of segment that has max comparison value smaller than
    // (largestSeenComparisonValue - TTL), i.e. out of metadata TTL. The expired metadata is removed while creating
    // new consuming segment in batches.
    boolean skipRemoveMetadata = false;
    if (_metadataTTL > 0 && _largestSeenComparisonValue.get() > 0) {
      Number maxComparisonValue =
          (Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0)).getMaxValue();
      if (maxComparisonValue.doubleValue() < _largestSeenComparisonValue.get() - _metadataTTL) {
        _logger.info("Skip removing segment: {} because it's out of TTL", segmentName);
        skipRemoveMetadata = true;
      }
    }
    if (!startOperation()) {
      _logger.info("Skip removing segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      if (!skipRemoveMetadata) {
        doRemoveSegment(segment);
      }
      _trackedSegments.remove(segment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
    }
  }

  protected void doRemoveSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing {} segment: {}, current primary key count: {}",
        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds =
        segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;
    if (validDocIds == null || validDocIds.isEmpty()) {
      _logger.info("Skip removing segment without valid docs: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      _logger.info("Removing {} primary keys for segment: {}", validDocIds.getCardinality(), segmentName);
      removeSegment(segment, validDocIds);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    updatePrimaryKeyGauge(numPrimaryKeys);
    _logger.info("Finished removing segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    // Directly return the record when partial-upsert is not enabled
    if (_partialUpsertHandler == null) {
      return record;
    }
    if (!startOperation()) {
      _logger.debug("Skip updating record because metadata manager is already stopped");
      return record;
    }
    try {
      return doUpdateRecord(record, recordInfo);
    } finally {
      finishOperation();
    }
  }

  protected abstract GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo);

  protected void handleOutOfOrderEvent(Object currentComparisonValue, Object recordComparisonValue) {
    boolean isPartialUpsertTable = (_partialUpsertHandler != null);
    _serverMetrics.addMeteredTableValue(_tableNameWithType,
        isPartialUpsertTable ? ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER : ServerMeter.UPSERT_OUT_OF_ORDER, 1L);
    _numOutOfOrderEvents++;
    long currentTimeNs = System.nanoTime();
    if (currentTimeNs - _lastOutOfOrderEventReportTimeNs > OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS) {
      _logger.warn("Skipped {} out-of-order events for {} upsert table {} (the last event has current comparison "
              + "value: {}, record comparison value: {})", _numOutOfOrderEvents,
          (isPartialUpsertTable ? "partial" : "full"), _tableNameWithType, currentComparisonValue,
          recordComparisonValue);
      _lastOutOfOrderEventReportTimeNs = currentTimeNs;
      _numOutOfOrderEvents = 0;
    }
  }

  /**
   * When we have to process a new segment, if there are comparison value ties for the same primary-key within the
   * segment, then for Partial Upsert tables we need to make sure that the record location map is updated only
   * for the latest version of the record. This is specifically a concern for Partial Upsert tables because Realtime
   * consumption can potentially end up reading the wrong version of a record, which will lead to permanent
   * data-inconsistency.
   *
   * <p>
   *  This function returns an iterator that will de-dup records with the same primary-key. Moreover, for comparison
   *  ties, it will only keep the latest record. This iterator can then further be used to update the primary-key
   *  record location map safely.
   * </p>
   *
   * @param recordInfoIterator iterator over the new segment
   * @param hashFunction       hash function configured for Upsert's primary keys
   * @return iterator that returns de-duplicated records. To resolve ties for comparison column values, we prefer to
   *         return the latest record.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static Iterator<RecordInfo> resolveComparisonTies(Iterator<RecordInfo> recordInfoIterator,
      HashFunction hashFunction) {
    Map<Object, RecordInfo> deDuplicatedRecordInfo = new HashMap<>();
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      Comparable newComparisonValue = recordInfo.getComparisonValue();
      deDuplicatedRecordInfo.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), hashFunction),
          (key, maxComparisonValueRecordInfo) -> {
            if (maxComparisonValueRecordInfo == null) {
              return recordInfo;
            }
            int comparisonResult = newComparisonValue.compareTo(maxComparisonValueRecordInfo.getComparisonValue());
            if (comparisonResult >= 0) {
              return recordInfo;
            }
            return maxComparisonValueRecordInfo;
          });
    }
    return deDuplicatedRecordInfo.values().iterator();
  }

  @Override
  public void takeSnapshot() {
    if (!_enableSnapshot) {
      return;
    }
    if (!_gotFirstConsumingSegment) {
      _logger.info("Skip taking snapshot before getting the first consuming segment");
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip taking snapshot because metadata manager is already stopped");
      return;
    }
    _snapshotLock.writeLock().lock();
    try {
      long startTime = System.currentTimeMillis();
      doTakeSnapshot();
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.UPSERT_SNAPSHOT_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    } finally {
      _snapshotLock.writeLock().unlock();
      finishOperation();
    }
  }

  // TODO: Consider optimizing it by tracking and persisting only the changed snapshot
  protected void doTakeSnapshot() {
    int numTrackedSegments = _trackedSegments.size();
    long numPrimaryKeysInSnapshot = 0L;
    _logger.info("Taking snapshot for {} segments", numTrackedSegments);
    long startTimeMs = System.currentTimeMillis();

    int numImmutableSegments = 0;
    int numConsumingSegments = 0;
    // The segments without validDocIds snapshots should take their snapshots at last. So that when there is failure
    // to take snapshots, the validDocIds snapshot on disk still keep track of an exclusive set of valid docs across
    // segments. Because the valid docs as tracked by the existing validDocIds snapshots can only get less. That no
    // overlap of valid docs among segments with snapshots is required by the preloading to work correctly.
    Set<ImmutableSegmentImpl> segmentsWithoutSnapshot = new HashSet<>();
    for (IndexSegment segment : _trackedSegments) {
      if (!(segment instanceof ImmutableSegmentImpl)) {
        numConsumingSegments++;
        continue;
      }
      ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) segment;
      if (!immutableSegment.hasValidDocIdsSnapshotFile()) {
        segmentsWithoutSnapshot.add(immutableSegment);
        continue;
      }
      immutableSegment.persistValidDocIdsSnapshot();
      numImmutableSegments++;
      numPrimaryKeysInSnapshot += immutableSegment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
    }
    for (ImmutableSegmentImpl segment : segmentsWithoutSnapshot) {
      segment.persistValidDocIdsSnapshot();
      numImmutableSegments++;
      numPrimaryKeysInSnapshot += segment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
    }

    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, numImmutableSegments);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, numPrimaryKeysInSnapshot);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT,
        numTrackedSegments - numImmutableSegments - numConsumingSegments);
    _logger.info("Finished taking snapshot for {} immutable segments with {} primary keys (out of {} total segments, "
            + "{} are consuming segments) in {} ms", numImmutableSegments, numPrimaryKeysInSnapshot, numTrackedSegments,
        numConsumingSegments, System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Loads watermark from the file if exists.
   */
  protected double loadWatermark() {
    File watermarkFile = getWatermarkFile();
    if (watermarkFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(watermarkFile);
        double watermark = ByteBuffer.wrap(bytes).getDouble();
        _logger.info("Loaded watermark: {} from file for table: {} partition_id: {}", watermark, _tableNameWithType,
            _partitionId);
        return watermark;
      } catch (Exception e) {
        _logger.warn("Caught exception while loading watermark file: {}, skipping", watermarkFile);
      }
    }
    return Double.MIN_VALUE;
  }

  /**
   * Persists watermark to the file.
   */
  protected void persistWatermark(double watermark) {
    File watermarkFile = getWatermarkFile();
    try {
      if (watermarkFile.exists()) {
        if (!FileUtils.deleteQuietly(watermarkFile)) {
          _logger.warn("Cannot delete watermark file: {}, skipping", watermarkFile);
          return;
        }
      }
      try (OutputStream outputStream = new FileOutputStream(watermarkFile, false);
          DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeDouble(watermark);
      }
      _logger.info("Persisted watermark: {} to file: {}", watermark, watermarkFile);
    } catch (Exception e) {
      _logger.warn("Caught exception while persisting watermark file: {}, skipping", watermarkFile);
    }
  }

  /**
   * Deletes the watermark file.
   */
  protected void deleteWatermark() {
    File watermarkFile = getWatermarkFile();
    if (watermarkFile.exists()) {
      if (!FileUtils.deleteQuietly(watermarkFile)) {
        _logger.warn("Cannot delete watermark file: {}, skipping", watermarkFile);
      }
    }
  }

  protected File getWatermarkFile() {
    return new File(_tableIndexDir, V1Constants.TTL_WATERMARK_TABLE_PARTITION + _partitionId);
  }

  @Override
  public void removeExpiredPrimaryKeys() {
    if (_metadataTTL <= 0 && _deletedKeysTTL <= 0) {
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip removing expired primary keys because metadata manager is already stopped");
      return;
    }
    try {
      long startTime = System.currentTimeMillis();
      doRemoveExpiredPrimaryKeys();
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.UPSERT_REMOVE_EXPIRED_PRIMARY_KEYS_TIME_MS,
          duration, TimeUnit.MILLISECONDS);
    } finally {
      finishOperation();
    }
  }

  /**
   * Removes all primary keys that have comparison value smaller than (largestSeenComparisonValue - TTL).
   */
  protected abstract void doRemoveExpiredPrimaryKeys();

  protected synchronized boolean startOperation() {
    if (_stopped || _numPendingOperations == 0) {
      return false;
    }
    _numPendingOperations++;
    return true;
  }

  protected synchronized void finishOperation() {
    _numPendingOperations--;
    if (_numPendingOperations == 0) {
      notifyAll();
    }
  }

  @Override
  public synchronized void stop() {
    if (_stopped) {
      _logger.warn("Metadata manager is already stopped");
      return;
    }
    _stopped = true;
    _numPendingOperations--;
    _logger.info("Stopped the metadata manager with {} pending operations, current primary key count: {}",
        _numPendingOperations, getNumPrimaryKeys());
  }

  @Override
  public synchronized void close()
      throws IOException {
    Preconditions.checkState(_stopped, "Must stop the metadata manager before closing it");
    if (_closed) {
      _logger.warn("Metadata manager is already closed");
      return;
    }
    _closed = true;
    _logger.info("Closing the metadata manager");
    while (_numPendingOperations != 0) {
      _logger.info("Waiting for {} pending operations to finish", _numPendingOperations);
      try {
        wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(
            String.format("Interrupted while waiting for %d pending operations to finish", _numPendingOperations), e);
      }
    }
    doClose();
    // We don't remove the segment from the metadata manager when
    // it's closed. This was done to make table deletion faster. Since we don't remove the segment, we never decrease
    // the primary key count. So, we set the primary key count to 0 here.
    updatePrimaryKeyGauge(0);
    _logger.info("Closed the metadata manager");
  }

  protected void doClose()
      throws IOException {
  }
}
