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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public abstract class BaseRetentionPartitionDedupMetadataManager
    extends BasePartitionDedupMetadataManager {
  protected final double _metadataTTL;
  protected final String _metadataTimeColumn;

  protected BaseRetentionPartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns,
      int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction, double metadataTTL,
      String metadataTimeColumn) {
    super(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction);
    Preconditions.checkArgument(metadataTTL > 0, "metadataTTL: %s for table: %s must be positive when "
        + "RetentionConcurrentMapPartitionDedupMetadataManager is used", metadataTTL, tableNameWithType);
    _metadataTTL = metadataTTL;
    Preconditions.checkArgument(metadataTimeColumn != null,
        "When metadataTTL is configured, metadata time column must be configured for dedup enabled table: %s",
        tableNameWithType);
    _metadataTimeColumn = metadataTimeColumn;
  }

  @Override
  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    throw new UnsupportedOperationException("checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) is "
        + "not supported for Retention enabled PartitionDedupMetadataManager");
  }

  @Override
  public void addSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _metadataTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      addSegment(segment, dedupRecordInfoIterator);
      int dedupPrimaryKeyCount = removeExpiredPrimaryKeys();
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          dedupPrimaryKeyCount);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while adding segment: %s of table: %s to "
              + "RetentionConcurrentMapPartitionDedupMetadataManager",
          segment.getSegmentName(), _tableNameWithType), e);
    }
  }

  @VisibleForTesting
  protected abstract void addSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator);

  @Override
  public void removeSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _metadataTimeColumn)) {
      Iterator<DedupRecordInfo> dedupRecordInfoIterator =
          DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      removeSegment(segment, dedupRecordInfoIterator);
      int dedupPrimaryKeyCount = removeExpiredPrimaryKeys();
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          dedupPrimaryKeyCount);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while removing segment: %s of table: %s from "
              + "RetentionConcurrentMapPartitionDedupMetadataManager",
          segment.getSegmentName(), _tableNameWithType), e);
    }
  }

  @VisibleForTesting
  protected abstract void removeSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator);
}
