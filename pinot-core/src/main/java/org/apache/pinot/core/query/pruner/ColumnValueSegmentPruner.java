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
package org.apache.pinot.core.query.pruner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


/// The `ColumnValueSegmentPruner` is the segment pruner that prunes segments based on the value inside the
/// filter. This pruner is supposed to use segment metadata like min/max or partition id only. Pruners that need to
/// access segment data like bloom filter is implemented separately and called after this one to reduce required data
/// access.
///
/// - For EQUALITY filter, prune the segment based on:
///   - Column min/max value
///   - Column partition
/// - For RANGE filter, prune the segment based on:
///   - Column min/max value<
@SuppressWarnings({"rawtypes", "unchecked", "RedundantIfStatement"})
public class ColumnValueSegmentPruner extends ValueBasedSegmentPruner {
  @Override
  protected boolean isApplicableToPredicate(Predicate predicate, Map<String, String> queryOptions) {
    // Only prune columns
    if (predicate.getLhs().getType() != ExpressionContext.Type.IDENTIFIER) {
      return false;
    }
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ || predicateType == Predicate.Type.RANGE) {
      return true;
    }
    if (predicateType == Predicate.Type.IN) {
      List<String> values = ((InPredicate) predicate).getValues();
      if (shouldPruneInPredicate(values.size(), queryOptions)) {
        return true;
      }
    }
    return false;
  }

  @Override
  boolean pruneSegmentWithPredicate(IndexSegment segment, Predicate predicate, Map<String, DataSource> dataSourceCache,
      ValueCache cachedValues, QueryContext query) {
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ) {
      return pruneEqPredicate(segment, (EqPredicate) predicate, dataSourceCache, cachedValues, query);
    } else if (predicateType == Predicate.Type.IN) {
      return pruneInPredicate(segment, (InPredicate) predicate, dataSourceCache, cachedValues, query);
    } else if (predicateType == Predicate.Type.RANGE) {
      return pruneRangePredicate(segment, (RangePredicate) predicate, dataSourceCache, cachedValues, query);
    } else {
      return false;
    }
  }

  /// For EQ predicate, prune the segments based on:
  ///
  /// - Column min/max value
  /// - Column partition
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache, QueryContext query) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column, query.getSchema())
        : dataSourceCache.computeIfAbsent(column, col -> segment.getDataSource(column, query.getSchema()));
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    ValueCache.CachedValue cachedValue = valueCache.get(eqPredicate, dataSourceMetadata.getDataType());
    // Check min/max value
    Comparable value = cachedValue.getComparableValue();
    if (dataSourceMetadata.isOutsideMinMaxRange(value, true, value, true)) {
      return true;
    }
    // Check column partition
    PartitionFunction partitionFunction = dataSourceMetadata.getPartitionFunction();
    if (partitionFunction != null) {
      Set<Integer> partitions = dataSourceMetadata.getPartitions();
      assert partitions != null;
      if (!partitions.contains(partitionFunction.getPartition(cachedValue.getValue()))) {
        return true;
      }
    }
    return false;
  }

  /// For IN predicate, prune the segments based on:
  ///
  /// - Column min/max value
  ///
  private boolean pruneInPredicate(IndexSegment segment, InPredicate inPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache, QueryContext query) {
    List<String> values = inPredicate.getValues();
    if (!shouldPruneInPredicate(values.size(), query.getQueryOptions())) {
      return false;
    }
    String column = inPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column, query.getSchema())
        : dataSourceCache.computeIfAbsent(column, col -> segment.getDataSource(column, query.getSchema()));
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    return dataSourceMetadata.isOutsideMinMaxRange(
        valueCache.getComparableValues(inPredicate, dataSourceMetadata.getDataType()));
  }

  /// For RANGE predicate, prune the segments based on:
  ///
  /// - Column min/max value
  private boolean pruneRangePredicate(IndexSegment segment, RangePredicate rangePredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache, QueryContext query) {
    String column = rangePredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column, query.getSchema())
        : dataSourceCache.computeIfAbsent(column, col -> segment.getDataSource(column, query.getSchema()));
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();

    ValueCache.CachedRange range = valueCache.get(rangePredicate, dataSourceMetadata.getDataType());
    return range.empty() || dataSourceMetadata.isOutsideMinMaxRange(
        range.lower(), rangePredicate.isLowerInclusive(), range.upper(), rangePredicate.isUpperInclusive());
  }
}
