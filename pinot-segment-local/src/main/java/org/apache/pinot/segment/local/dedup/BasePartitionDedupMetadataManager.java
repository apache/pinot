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
package org.apache.pinot.segment.local.dedup;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BasePartitionDedupMetadataManager implements PartitionDedupMetadataManager {
  protected final String _tableNameWithType;
  protected final List<String> _primaryKeyColumns;
  protected final int _partitionId;
  protected final ServerMetrics _serverMetrics;
  protected final HashFunction _hashFunction;
  protected final double _metadataTTL;
  protected final String _dedupTimeColumn;
  protected final AtomicDouble _largestSeenTime = new AtomicDouble(0);
  protected final Logger _logger;
  // The following variables are always accessed within synchronized block
  private boolean _stopped;
  // Initialize with 1 pending operation to indicate the metadata manager can take more operations
  private int _numPendingOperations = 1;
  private boolean _closed;

  protected BasePartitionDedupMetadataManager(String tableNameWithType, int partitionId, DedupContext dedupContext) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = dedupContext.getPrimaryKeyColumns();
    _hashFunction = dedupContext.getHashFunction();
    _serverMetrics = dedupContext.getServerMetrics();
    _metadataTTL = dedupContext.getMetadataTTL() >= 0 ? dedupContext.getMetadataTTL() : 0;
    _dedupTimeColumn = dedupContext.getDedupTimeColumn();
    if (_metadataTTL > 0) {
      Preconditions.checkArgument(_dedupTimeColumn != null,
          "When metadataTTL is configured, metadata time column must be configured for dedup enabled table: %s",
          tableNameWithType);
    }
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
  }

  @Override
  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    throw new UnsupportedOperationException(
        "checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) is " + "deprecated!");
  }

  @Override
  public void addSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: %s for segment: %s, table: %s", segment.getClass(), segmentName,
        _tableNameWithType);
    // If metadataTTL is enabled, we can skip adding segment that's already getting out of the TTL.
    if (_metadataTTL > 0) {
      double maxDedupTime = ((Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_dedupTimeColumn)
          .getMaxValue()).doubleValue();
      _largestSeenTime.getAndUpdate(time -> Math.max(time, maxDedupTime));
      if (isOutOfMetadataTTL(maxDedupTime)) {
        _logger.info("Skip adding segment: {} as max dedupTime: {} is out of metadataTTL: {}", segmentName,
            _dedupTimeColumn, _metadataTTL);
        return;
      }
    }
    if (!startOperation()) {
      _logger.info("Skip adding segment: {} because dedup metadata manager is already stopped",
          segment.getSegmentName());
      return;
    }
    try {
      addOrReplaceSegment(null, segment);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while adding segment: %s of table: %s to %s", segment.getSegmentName(),
              _tableNameWithType, this.getClass().getSimpleName()), e);
    } finally {
      finishOperation();
    }
  }

  @Override
  public void replaceSegment(IndexSegment oldSegment, IndexSegment newSegment) {
    if (!startOperation()) {
      _logger.info("Skip replacing segment: {} with segment: {} because dedup metadata manager is already stopped",
          oldSegment.getSegmentName(), newSegment.getSegmentName());
      return;
    }
    try {
      addOrReplaceSegment(oldSegment, newSegment);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while replacing segment: %s with segment: %s of table: %s in %s",
              oldSegment.getSegmentName(), newSegment.getSegmentName(), _tableNameWithType,
              this.getClass().getSimpleName()), e);
    } finally {
      finishOperation();
    }
  }

  private void addOrReplaceSegment(@Nullable IndexSegment oldSegment, IndexSegment newSegment)
      throws IOException {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(newSegment,
        _primaryKeyColumns, _dedupTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, newSegment.getSegmentMetadata().getTotalDocs());
      doAddOrReplaceSegment(oldSegment, newSegment, dedupRecordInfoIterator);
      updatePrimaryKeyGauge();
    }
  }

  /**
   * Adds the dedup metadata for the new segment if old segment is null; or replaces the dedup metadata for the given
   * old segment with the new segment if the old segment is not null.
   * @param oldSegment The old segment to replace. If null, add the new segment.
   * @param newSegment The new segment to add or replace.
   * @param dedupRecordInfoIteratorOfNewSegment The iterator of dedup record info of the new segment.
   */
  protected abstract void doAddOrReplaceSegment(@Nullable IndexSegment oldSegment, IndexSegment newSegment,
      Iterator<DedupRecordInfo> dedupRecordInfoIteratorOfNewSegment);

  @Override
  public void removeSegment(IndexSegment segment) {
    if (!startOperation()) {
      _logger.info("Skip removing segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _dedupTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      doRemoveSegment(segment, dedupRecordInfoIterator);
      updatePrimaryKeyGauge();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while removing segment: %s of table: %s from %s", segment.getSegmentName(),
              _tableNameWithType, this.getClass().getSimpleName()), e);
    } finally {
      finishOperation();
    }
  }

  protected abstract void doRemoveSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator);

  @Override
  public void removeExpiredPrimaryKeys() {
    if (!startOperation()) {
      _logger.info("Skip removing expired primary keys because metadata manager is already stopped");
      return;
    }
    try {
      long startTime = System.currentTimeMillis();
      doRemoveExpiredPrimaryKeys();
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(_tableNameWithType, ServerTimer.DEDUP_REMOVE_EXPIRED_PRIMARY_KEYS_TIME_MS,
          duration, TimeUnit.MILLISECONDS);
    } finally {
      finishOperation();
    }
  }

  /**
   * Removes all primary keys that have dedup time smaller than (largestSeenDedupTime - TTL).
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

  protected boolean isOutOfMetadataTTL(double dedupTime) {
    return _metadataTTL > 0 && dedupTime < _largestSeenTime.get() - _metadataTTL;
  }

  protected abstract long getNumPrimaryKeys();

  protected void updatePrimaryKeyGauge(long numPrimaryKeys) {
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);
  }

  protected void updatePrimaryKeyGauge() {
    updatePrimaryKeyGauge(getNumPrimaryKeys());
  }

  protected void doClose()
      throws IOException {
  }
}
