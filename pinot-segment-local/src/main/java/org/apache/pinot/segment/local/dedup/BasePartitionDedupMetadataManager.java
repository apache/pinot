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
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
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
  protected final Logger _logger;

  protected BasePartitionDedupMetadataManager(String tableNameWithType, int partitionId,
      DedupContext dedupContext) {
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
    throw new UnsupportedOperationException("checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) is "
        + "deprecated!");
  }

  @Override
  public void addSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _dedupTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      addSegment(segment, dedupRecordInfoIterator);
      int dedupPrimaryKeyCount = removeExpiredPrimaryKeys();
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          dedupPrimaryKeyCount);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while adding segment: %s of table: %s to %s",
          segment.getSegmentName(), _tableNameWithType, this.getClass().getSimpleName()), e);
    }
  }

  protected abstract void addSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator);

  @Override
  public void removeSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _dedupTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      removeSegment(segment, dedupRecordInfoIterator);
      int dedupPrimaryKeyCount = removeExpiredPrimaryKeys();
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          dedupPrimaryKeyCount);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while removing segment: %s of table: %s from %s",
          segment.getSegmentName(), _tableNameWithType, this.getClass().getSimpleName()), e);
    }
  }

  protected abstract void removeSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator);
}
