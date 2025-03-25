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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.SegmentPreloadUtils;
import org.apache.pinot.segment.local.utils.WatermarkUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BasePartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  protected static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);
  // The special value to indicate the largest comparison value is not set yet, and allow negative comparison values.
  protected static final double TTL_WATERMARK_NOT_SET = Double.NEGATIVE_INFINITY;

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

  // Tracks all the segments managed by this manager, excluding EmptySegment and segments out of metadata TTL.
  // Basically, it's possible that some segments in the table partition are not tracked here, as their upsert metadata
  // is not managed by the manager currently.
  protected final Set<IndexSegment> _trackedSegments = ConcurrentHashMap.newKeySet();
  // Track all the immutable segments where changes took place since last snapshot was taken.
  // Note: we need take to take _snapshotLock RLock while updating this set as it may be updated by the multiple
  // Helix threads. Otherwise, segments might be missed by the consuming thread when taking snapshots, which takes
  // snapshotLock WLock and clear the tracking set to avoid keeping segment object references around.
  // Skip mutableSegments as only immutable segments are for taking snapshots.
  protected final Set<IndexSegment> _updatedSegmentsSinceLastSnapshot = ConcurrentHashMap.newKeySet();

  // NOTE: We do not persist snapshot on the first consuming segment because most segments might not be loaded yet
  // We only do this for Full-Upsert tables, for partial-upsert tables, we have a check allSegmentsLoaded
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

  // By default, the upsert consistency mode is NONE and upsertViewManager is disabled.
  private final UpsertViewManager _upsertViewManager;

  // We track newly added segments to get them included in the list of selected segments for queries to get a more
  // complete upsert data view, e.g. the newly created consuming segment or newly uploaded immutable segments. Such
  // segments can be processed by the server even before they get included in the broker's routing table. Server can
  // remove a segment from this map if it knows the segment has been included in the broker's routing table. But
  // there is no easy or efficient way for server to know if this happens on all brokers, so we track the segments
  // for a configurable period, to wait for brokers to add the segment in routing tables.
  private final Map<String, Long> _newlyAddedSegments = new ConcurrentHashMap<>();
  private final long _newSegmentTrackingTimeMs;

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
    _isPreloading = context.isPreloadEnabled();
    _metadataTTL = context.getMetadataTTL();
    _deletedKeysTTL = context.getDeletedKeysTTL();
    _tableIndexDir = context.getTableIndexDir();
    long trackingTimeMs = context.getNewSegmentTrackingTimeMs();
    UpsertConfig.ConsistencyMode cmode = context.getConsistencyMode();
    if (cmode == UpsertConfig.ConsistencyMode.SYNC || cmode == UpsertConfig.ConsistencyMode.SNAPSHOT) {
      _upsertViewManager = new UpsertViewManager(cmode, context);
      // For consistency mode, we have to track newly added segments, so use default tracking time to enable the
      // tracking of newly added segments if it's not enabled explicitly. This is for existing tables to work.
      // New tables are required to set a positive newSegmentTrackingTimeMs when to enable consistency mode.
      _newSegmentTrackingTimeMs =
          trackingTimeMs > 0 ? trackingTimeMs : UpsertViewManager.DEFAULT_NEW_SEGMENT_TRACKING_TIME_MS;
    } else {
      _upsertViewManager = null;
      _newSegmentTrackingTimeMs = trackingTimeMs;
    }
    _serverMetrics = ServerMetrics.get();
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
    if (isTTLEnabled()) {
      Preconditions.checkState(_comparisonColumns.size() == 1,
          "Upsert TTL does not work with multiple comparison columns");
      Preconditions.checkState(_metadataTTL <= 0 || _enableSnapshot, "Upsert metadata TTL must have snapshot enabled");
      _largestSeenComparisonValue =
          new AtomicDouble(WatermarkUtils.loadWatermark(getWatermarkFile(), TTL_WATERMARK_NOT_SET));
    } else {
      _largestSeenComparisonValue = new AtomicDouble(TTL_WATERMARK_NOT_SET);
      WatermarkUtils.deleteWatermark(getWatermarkFile());
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
      long startTime = System.currentTimeMillis();
      doPreloadSegments(tableDataManager, indexLoadingConfig, helixManager, segmentPreloadExecutor);
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.UPSERT_PRELOAD_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // We should continue even if preloading fails, so that segments not being preloaded successfully can get
      // loaded via the normal segment loading logic as done on the Helix task threads although with more costly
      // checks on the upsert metadata.
      _logger.warn("Failed to preload segments from partition: {} of table: {}, skipping", _partitionId,
          _tableNameWithType, e);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UPSERT_PRELOAD_FAILURE, 1);
      if (e instanceof InterruptedException) {
        // Restore the interrupted status in case the upper callers want to check.
        Thread.currentThread().interrupt();
      }
    } finally {
      _isPreloading = false;
      _preloadLock.unlock();
    }
  }

  // Keep this hook method for subclasses to extend the preloading logic as needed.
  protected void doPreloadSegments(TableDataManager tableDataManager, IndexLoadingConfig indexLoadingConfig,
      HelixManager helixManager, ExecutorService segmentPreloadExecutor)
      throws Exception {
    TableConfig tableConfig = indexLoadingConfig.getTableConfig();
    SegmentPreloadUtils.preloadSegments(tableDataManager, _partitionId, indexLoadingConfig, helixManager,
        segmentPreloadExecutor, (segmentName, segmentZKMetadata) -> {
          String tier = segmentZKMetadata.getTier();
          if (SegmentPreloadUtils.hasValidDocIdsSnapshot(tableDataManager, tableConfig, segmentName, tier)) {
            return true;
          }
          _logger.info("Skip segment: {} on tier: {} as it has no validDocIds snapshot", segmentName, tier);
          return false;
        });
  }

  @Override
  public void addSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: %s for segment: %s, table: %s", segment.getClass(), segmentName,
        _tableNameWithType);
    if (!startOperation()) {
      _logger.info("Skip adding segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doAddSegment((ImmutableSegmentImpl) segment);
      _trackedSegments.add(segment);
      if (_enableSnapshot) {
        _updatedSegmentsSinceLastSnapshot.add(segment);
      }
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
    }
  }

  protected boolean isTTLEnabled() {
    return _metadataTTL > 0 || _deletedKeysTTL > 0;
  }

  protected double getMaxComparisonValue(IndexSegment segment) {
    return ((Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0))
        .getMaxValue()).doubleValue();
  }

  protected boolean isOutOfMetadataTTL(double maxComparisonValue) {
    return _metadataTTL > 0 && _largestSeenComparisonValue.get() != TTL_WATERMARK_NOT_SET
        && maxComparisonValue < _largestSeenComparisonValue.get() - _metadataTTL;
  }

  protected boolean isOutOfMetadataTTL(IndexSegment segment) {
    if (_metadataTTL > 0) {
      double maxComparisonValue = getMaxComparisonValue(segment);
      return isOutOfMetadataTTL(maxComparisonValue);
    }
    return false;
  }

  protected boolean skipAddSegmentOutOfTTL(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Skip adding segment: {} because it's out of TTL", segmentName);
    MutableRoaringBitmap validDocIdsSnapshot = segment.loadValidDocIdsFromSnapshot();
    if (validDocIdsSnapshot != null) {
      MutableRoaringBitmap queryableDocIds = getQueryableDocIds(segment, validDocIdsSnapshot);
      segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(validDocIdsSnapshot),
          queryableDocIds != null ? new ThreadSafeMutableRoaringBitmap(queryableDocIds) : null);
    } else {
      _logger.warn("Failed to find validDocIds snapshot to add segment: {} out of TTL, treating all docs as valid",
          segmentName);
    }
    // Return true if segment is skipped. This boolean value allows subclass to decide whether to skip.
    return true;
  }

  protected boolean skipPreloadSegmentOutOfTTL(ImmutableSegmentImpl segment, MutableRoaringBitmap validDocIdsSnapshot) {
    String segmentName = segment.getSegmentName();
    _logger.info("Skip preloading segment: {} because it's out of TTL", segmentName);
    MutableRoaringBitmap queryableDocIds = getQueryableDocIds(segment, validDocIdsSnapshot);
    segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(validDocIdsSnapshot),
        queryableDocIds != null ? new ThreadSafeMutableRoaringBitmap(queryableDocIds) : null);
    // Return true if segment is skipped. This boolean value allows subclass to decide whether to skip.
    return true;
  }

  protected void doAddSegment(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Adding segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());
    if (isTTLEnabled()) {
      double maxComparisonValue = getMaxComparisonValue(segment);
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, maxComparisonValue));
      if (isOutOfMetadataTTL(maxComparisonValue) && skipAddSegmentOutOfTTL(segment)) {
        return;
      }
    }
    long startTimeMs = System.currentTimeMillis();
    if (!_enableSnapshot) {
      segment.deleteValidDocIdsSnapshot();
    }
    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      Iterator<RecordInfo> recordInfoIterator =
          UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
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
        "Got unsupported segment implementation: %s for segment: %s, table: %s", segment.getClass(), segmentName,
        _tableNameWithType);
    if (!startOperation()) {
      _logger.info("Skip preloading segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    _snapshotLock.readLock().lock();
    try {
      doPreloadSegment((ImmutableSegmentImpl) segment);
      _trackedSegments.add(segment);
      _updatedSegmentsSinceLastSnapshot.add(segment);
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
    if (isTTLEnabled()) {
      double maxComparisonValue = getMaxComparisonValue(segment);
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, maxComparisonValue));
      if (isOutOfMetadataTTL(maxComparisonValue) && skipPreloadSegmentOutOfTTL(segment, validDocIds)) {
        return;
      }
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
    if (validDocIds == null) {
      validDocIds = new ThreadSafeMutableRoaringBitmap();
    }
    if (queryableDocIds == null && _deleteRecordColumn != null) {
      queryableDocIds = new ThreadSafeMutableRoaringBitmap();
    }
    addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
  }

  protected void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    if (_partialUpsertHandler != null) {
      recordInfoIterator = resolveComparisonTies(recordInfoIterator, _hashFunction);
    }
    doAddOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, oldSegment,
        validDocIdsForOldSegment);
  }

  protected abstract void doAddOrReplaceSegment(ImmutableSegmentImpl segment,
      ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
      Iterator<RecordInfo> recordInfoIterator, @Nullable IndexSegment oldSegment,
      @Nullable MutableRoaringBitmap validDocIdsForOldSegment);

  protected void addSegmentWithoutUpsert(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
  }

  /**
   * <li> When the replacing segment and current segment are of {@link LLCSegmentName} then the PK should resolve to
   * row in segment with higher sequence id.
   * <li> If either or both are not LLC segment, then resolve based on creation time of segment. If creation time is
   * same then prefer uploaded segment if other is LLCSegmentName
   * <li> If both are uploaded segment, prefer standard UploadedRealtimeSegmentName, if still a tie, then resolve to
   * current segment.
   *
   * @param segmentName replacing segment name
   * @param currentSegmentName current segment name having the record for the given primary key
   * @param segmentCreationTimeMs replacing segment creation time
   * @param currentSegmentCreationTimeMs current segment creation time
   * @return true if the record in replacing segment should replace the record in current segment
   */
  protected boolean shouldReplaceOnComparisonTie(String segmentName, String currentSegmentName,
      long segmentCreationTimeMs, long currentSegmentCreationTimeMs) {
    // resolve using sequence id if both are LLCSegmentName
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    LLCSegmentName currentLLCSegmentName = LLCSegmentName.of(currentSegmentName);
    if (llcSegmentName != null && currentLLCSegmentName != null) {
      return llcSegmentName.getSequenceNumber() > currentLLCSegmentName.getSequenceNumber();
    }

    // either or both are uploaded segments, prefer the latest segment
    int creationTimeComparisonRes = Long.compare(segmentCreationTimeMs, currentSegmentCreationTimeMs);
    if (creationTimeComparisonRes != 0) {
      return creationTimeComparisonRes > 0;
    }

    // if both are uploaded segment, prefer standard UploadedRealtimeSegmentName, if still a tie, then resolve to
    // current segment
    if (UploadedRealtimeSegmentName.of(currentSegmentName) != null) {
      return false;
    }
    return UploadedRealtimeSegmentName.of(segmentName) != null;
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
        if (_enableSnapshot) {
          _updatedSegmentsSinceLastSnapshot.add(segment);
        }
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
        "Cannot replace segment with different name for table: %s, old segment: %s, new segment: %s",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing {} segment: {}, current primary key count: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      replaceSegment(segment, null, null, null, oldSegment);
      return;
    }
    if (isTTLEnabled()) {
      double maxComparisonValue = getMaxComparisonValue(segment);
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, maxComparisonValue));
      // Segment might be uploaded directly to the table to replace an old segment. So update the TTL watermark but
      // we can't skip segment even if it's out of TTL as its validDocIds bitmap is not updated yet.
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
    // Currently when TTL is enabled, we don't support skip loading out-of-TTL segment with snapshots, since we don't
    // know which docs are valid in the new segment.
    // TODO: when ttl is enabled, we can allow
    //       (1) skip loading segments without any invalid docs.
    //       (2) assign the invalid docs from the replaced segment to the new segment.
    String segmentName = segment.getSegmentName();
    MutableRoaringBitmap validDocIdsForOldSegment = null;
    if (_upsertViewManager == null) {
      // When not using consistency mode, we use a copy of the validDocIds bitmap of the old segment to keep the old
      // segment intact during segment replacement and queries access the old segment during segment replacement.
      validDocIdsForOldSegment = getValidDocIdsForOldSegment(oldSegment);
    }
    if (recordInfoIterator != null) {
      Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
          "Got unsupported segment implementation: %s for segment: %s, table: %s", segment.getClass(), segmentName,
          _tableNameWithType);
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      if (queryableDocIds == null && _deleteRecordColumn != null) {
        queryableDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, queryableDocIds, recordInfoIterator, oldSegment,
          validDocIdsForOldSegment);
    }
    if (_upsertViewManager != null) {
      // When using consistency mode, the old segment's bitmap is updated in place, so we get the validDocIds after
      // segment replacement is done.
      validDocIdsForOldSegment = getValidDocIdsForOldSegment(oldSegment);
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
        _logger.info("Found {} primary keys not replaced when replacing segment: {}", numKeysNotReplaced, segmentName);
      }
      removeSegment(oldSegment, validDocIdsForOldSegment);
    }
  }

  private MutableRoaringBitmap getValidDocIdsForOldSegment(IndexSegment oldSegment) {
    return oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
  }

  protected void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    try (PrimaryKeyReader primaryKeyReader = new PrimaryKeyReader(segment, _primaryKeyColumns)) {
      removeSegment(segment, UpsertUtils.getPrimaryKeyIterator(primaryKeyReader, validDocIds));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while removing segment: %s, table: %s", segment.getSegmentName(),
              _tableNameWithType), e);
    }
  }

  protected void removeSegment(IndexSegment segment, Iterator<PrimaryKey> primaryKeyIterator) {
    throw new UnsupportedOperationException("Both removeSegment(segment, validDocID) and "
        + "removeSegment(segment, pkIterator) are not implemented. Implement one of them to support removal.");
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    if (!_trackedSegments.contains(segment)) {
      _logger.info("Skip removing untracked (replaced or empty) segment: {}", segmentName);
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip removing segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      // Skip removing the upsert metadata of segment that is out of metadata TTL. The expired metadata is removed
      // while creating new consuming segment in batches.
      if (isOutOfMetadataTTL(segment)) {
        _logger.info("Skip removing segment: {} because it's out of TTL", segmentName);
      } else {
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

    _logger.info("Removing {} primary keys for segment: {}", validDocIds.getCardinality(), segmentName);
    removeSegment(segment, validDocIds);

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
    if (_partialUpsertHandler == null && !_gotFirstConsumingSegment) {
      // We only skip for full-Upsert tables, for partial-upsert tables, we have a check allSegmentsLoaded in
      // RealtimeTableDataManager
      _logger.info("Skip taking snapshot before getting the first consuming segment for full-upsert table");
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip taking snapshot because metadata manager is already stopped");
      return;
    }

    try {
      long startTime = System.currentTimeMillis();
      while (!_snapshotLock.writeLock().tryLock(5, TimeUnit.MINUTES)) {
        _logger.warn("Unable to acquire snapshotLock.writeLock in: {}. Retrying.", System.currentTimeMillis() - startTime);
      }
      try {
        _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.UPSERT_SNAPSHOT_WRITE_LOCK_TIME_MS,
            System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);

        startTime = System.currentTimeMillis();
        doTakeSnapshot();
        _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.UPSERT_SNAPSHOT_TIME_MS,
            System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        _logger.warn("Caught exception while taking snapshot", e);
      } finally {
        _snapshotLock.writeLock().unlock();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      finishOperation();
    }
  }

  protected void doTakeSnapshot() {
    int numTrackedSegments = _trackedSegments.size();
    long numPrimaryKeysInSnapshot = 0L;
    _logger.info("Taking snapshot for {} segments", numTrackedSegments);
    long startTimeMs = System.currentTimeMillis();

    int numImmutableSegments = 0;
    int numConsumingSegments = 0;
    int numUnchangedSegments = 0;
    // The segments without validDocIds snapshots should take their snapshots at last. So that when there is failure
    // to take snapshots, the validDocIds snapshot on disk still keep track of an exclusive set of valid docs across
    // segments. Because the valid docs as tracked by the existing validDocIds snapshots can only get less. That no
    // overlap of valid docs among segments with snapshots is required by the preloading to work correctly.
    Set<ImmutableSegmentImpl> segmentsWithoutSnapshot = new HashSet<>();
    TableDataManager tableDataManager = _context.getTableDataManager();
    boolean isSegmentSkipped = false;
    for (IndexSegment segment : _trackedSegments) {
      if (!(segment instanceof ImmutableSegmentImpl)) {
        numConsumingSegments++;
        continue;
      }
      if (!_updatedSegmentsSinceLastSnapshot.contains(segment)) {
        // if no updates since last snapshot then skip
        numUnchangedSegments++;
        continue;
      }
      // Try to acquire the segmentLock when taking snapshot for the segment because the segment directory can be
      // modified, e.g. a new snapshot file can be added to the directory. If not taking the lock, the Helix task
      // thread replacing the segment could fail. For example, we found FileUtils.cleanDirectory() failed due to
      // DirectoryNotEmptyException because a new snapshot file got added into the segment directory just between two
      // major cleanup steps in the cleanDirectory() method.
      String segmentName = segment.getSegmentName();
      Lock segmentLock = tableDataManager.getSegmentLock(segmentName);
      boolean locked = segmentLock.tryLock();
      if (!locked) {
        // Try to get the segmentLock in a non-blocking manner to avoid deadlock. The Helix task thread takes
        // segmentLock first and then the snapshot RLock when replacing a segment. However, the consuming thread has
        // already acquired the snapshot WLock when reaching here, and if it has to wait for segmentLock, it may
        // enter deadlock with the Helix task threads waiting for snapshot RLock.
        // If we can't get the segmentLock, we'd better skip taking snapshot for this tracked segment, because its
        // validDocIds might become stale or wrong when the segment is being processed by another thread right now.
        _logger.warn("Could not get segmentLock to take snapshot for segment: {}, skipping", segmentName);
        isSegmentSkipped = true;
        continue;
      }
      try {
        ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) segment;
        if (!immutableSegment.hasValidDocIdsSnapshotFile()) {
          segmentsWithoutSnapshot.add(immutableSegment);
          continue;
        }
        immutableSegment.persistValidDocIdsSnapshot();
        _updatedSegmentsSinceLastSnapshot.remove(segment);
        numImmutableSegments++;
        numPrimaryKeysInSnapshot += immutableSegment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
      } catch (Exception e) {
        _logger.warn("Caught exception while taking snapshot for segment: {}, skipping", segmentName, e);
        isSegmentSkipped = true;
      } finally {
        segmentLock.unlock();
      }
    }
    // If we have skipped any segments in the previous for-loop, we should skip the next for-loop, basically to not
    // add new snapshot files on disk. This ensures all the validDocIds snapshots kept on disk are still disjoint
    // with each other, although some of them may have become stale, i.e. tracking more valid docs than expected.
    if (!isSegmentSkipped) {
      for (ImmutableSegmentImpl segment : segmentsWithoutSnapshot) {
        String segmentName = segment.getSegmentName();
        Lock segmentLock = tableDataManager.getSegmentLock(segmentName);
        boolean locked = segmentLock.tryLock();
        if (!locked) {
          _logger.warn("Could not get segmentLock to take snapshot for segment: {} w/o snapshot, skipping",
              segmentName);
          continue;
        }
        try {
          segment.persistValidDocIdsSnapshot();
          _updatedSegmentsSinceLastSnapshot.remove(segment);
          numImmutableSegments++;
          numPrimaryKeysInSnapshot += segment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
        } catch (Exception e) {
          _logger.warn("Caught exception while taking snapshot for segment: {} w/o snapshot, skipping", segmentName, e);
        } finally {
          segmentLock.unlock();
        }
      }
    }
    _updatedSegmentsSinceLastSnapshot.retainAll(_trackedSegments);
    // Persist TTL watermark after taking snapshots if TTL is enabled, so that segments out of TTL can be loaded with
    // updated validDocIds bitmaps. If the TTL watermark is persisted first, segments out of TTL may get loaded with
    // stale bitmaps or even no bitmap snapshots to use.
    if (isTTLEnabled()) {
      WatermarkUtils.persistWatermark(_largestSeenComparisonValue.get(), getWatermarkFile());
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, numImmutableSegments);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, numPrimaryKeysInSnapshot);
    int numMissedSegments = numTrackedSegments - numImmutableSegments - numConsumingSegments - numUnchangedSegments;
    if (numMissedSegments > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, String.valueOf(_partitionId),
          ServerMeter.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT, numMissedSegments);
      _logger.warn("Missed taking snapshot for {} immutable segments", numMissedSegments);
    }
    _logger.info("Finished taking snapshot for {} immutable segments with {} primary keys (out of {} total segments, "
            + "{} are consuming segments) in {} ms", numImmutableSegments, numPrimaryKeysInSnapshot, numTrackedSegments,
        numConsumingSegments, System.currentTimeMillis() - startTimeMs);
  }

  protected File getWatermarkFile() {
    return new File(_tableIndexDir, V1Constants.TTL_WATERMARK_TABLE_PARTITION + _partitionId);
  }

  @VisibleForTesting
  double getWatermark() {
    return _largestSeenComparisonValue.get();
  }

  @VisibleForTesting
  void setWatermark(double watermark) {
    _largestSeenComparisonValue.set(watermark);
  }

  @Override
  public void removeExpiredPrimaryKeys() {
    if (!isTTLEnabled()) {
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

  /**
   * The same R/WLock is used by the two consistency modes, but they are independent:
   * - For sync mode, upsert threads take WLock to make updates on two segments' bitmaps atomically, and query threads
   *   take RLock when to access the segment bitmaps.
   * - For snapshot mode, upsert threads take RLock to make updates on segments' bitmaps so that they can be
   *   synchronized with threads taking the snapshot of bitmaps, which take the WLock.
   */
  protected void replaceDocId(IndexSegment newSegment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, IndexSegment oldSegment, int oldDocId, int newDocId,
      RecordInfo recordInfo) {
    if (_upsertViewManager == null) {
      UpsertUtils.doRemoveDocId(oldSegment, oldDocId);
      UpsertUtils.doAddDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
    } else {
      _upsertViewManager.replaceDocId(newSegment, validDocIds, queryableDocIds, oldSegment, oldDocId, newDocId,
          recordInfo);
    }
    trackUpdatedSegmentsSinceLastSnapshot(oldSegment);
  }

  /**
   *  There is no need to take the R/WLock to update single bitmap, as all methods to update the bitmap is synchronized.
   *  But for upsertViewBatchRefresh to work correctly, we need to block updates on bitmaps while doing batch refresh,
   *  which takes WLock. So wrap bitmap update logic inside RLock to allow concurrent updates but to be blocked when
   *  there is thread doing batch refresh, i.e. to take copies of all bitmaps.
   */
  protected void replaceDocId(IndexSegment segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, int oldDocId, int newDocId, RecordInfo recordInfo) {
    if (_upsertViewManager == null) {
      UpsertUtils.doReplaceDocId(validDocIds, queryableDocIds, oldDocId, newDocId, recordInfo);
    } else {
      _upsertViewManager.replaceDocId(segment, validDocIds, queryableDocIds, oldDocId, newDocId, recordInfo);
    }
  }

  protected void addDocId(IndexSegment segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, int docId, RecordInfo recordInfo) {
    if (_upsertViewManager == null) {
      UpsertUtils.doAddDocId(validDocIds, queryableDocIds, docId, recordInfo);
    } else {
      _upsertViewManager.addDocId(segment, validDocIds, queryableDocIds, docId, recordInfo);
    }
  }

  protected void removeDocId(IndexSegment segment, int docId) {
    if (_upsertViewManager == null) {
      UpsertUtils.doRemoveDocId(segment, docId);
    } else {
      _upsertViewManager.removeDocId(segment, docId);
    }
    trackUpdatedSegmentsSinceLastSnapshot(segment);
  }

  private void trackUpdatedSegmentsSinceLastSnapshot(IndexSegment segment) {
    if (_enableSnapshot && segment instanceof ImmutableSegment) {
      _snapshotLock.readLock().lock();
      try {
        _updatedSegmentsSinceLastSnapshot.add(segment);
      } finally {
        _snapshotLock.readLock().unlock();
      }
    }
  }

  protected void doClose()
      throws IOException {
  }

  public UpsertViewManager getUpsertViewManager() {
    return _upsertViewManager;
  }

  @Override
  public void trackSegmentForUpsertView(IndexSegment segment) {
    if (_upsertViewManager != null) {
      _upsertViewManager.trackSegment(segment);
    }
  }

  @Override
  public void untrackSegmentForUpsertView(IndexSegment segment) {
    if (_upsertViewManager != null) {
      _upsertViewManager.untrackSegment(segment);
    }
  }

  @Override
  public void trackNewlyAddedSegment(String segmentName) {
    if (_newSegmentTrackingTimeMs > 0) {
      _newlyAddedSegments.put(segmentName, System.currentTimeMillis() + _newSegmentTrackingTimeMs);
      if (_logger.isDebugEnabled()) {
        _logger.debug("Tracked newly added segments: {}", _newlyAddedSegments);
      }
    }
  }

  public Set<String> getNewlyAddedSegments() {
    if (_newSegmentTrackingTimeMs > 0) {
      // Untrack stale segments at query time. The overhead should be limited as the tracking map should be very small.
      long nowMs = System.currentTimeMillis();
      _newlyAddedSegments.values().removeIf(v -> v < nowMs);
      return _newlyAddedSegments.keySet();
    }
    return Collections.emptySet();
  }
}
