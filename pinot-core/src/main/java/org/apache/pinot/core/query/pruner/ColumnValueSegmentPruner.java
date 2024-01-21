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
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The {@code ColumnValueSegmentPruner} is the segment pruner that prunes segments based on the value inside the filter.
 * This pruner is supposed to use segment metadata like min/max or partition id only. Pruners that need to access
 * segment data like bloom filter is implemented separately and called after this one to reduce required data access.
 * <ul>
 *   <li>
 *     For EQUALITY filter, prune the segment based on:
 *     <ul>
 *       <li>Column min/max value</li>
 *       <li>Column partition</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For RANGE filter, prune the segment based on:
 *     <ul>
 *       <li>Column min/max value<</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked", "RedundantIfStatement"})
public class ColumnValueSegmentPruner extends ValueBasedSegmentPruner {
  @Override
  protected boolean isApplicableToPredicate(Predicate predicate) {
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
      // Skip pruning when there are too many values in the IN predicate
      if (values.size() <= _inPredicateThreshold) {
        return true;
      }
    }
    return false;
  }

  @Override
  boolean pruneSegmentWithPredicate(IndexSegment segment, Predicate predicate, Map<String, DataSource> dataSourceCache,
      ValueCache cachedValues) {
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ) {
      return pruneEqPredicate(segment, (EqPredicate) predicate, dataSourceCache, cachedValues);
    } else if (predicateType == Predicate.Type.IN) {
      return pruneInPredicate(segment, (InPredicate) predicate, dataSourceCache, cachedValues);
    } else if (predicateType == Predicate.Type.RANGE) {
      return pruneRangePredicate(segment, (RangePredicate) predicate, dataSourceCache);
    } else {
      return false;
    }
  }

  /**
   * For EQ predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   *   <li>Column partition</li>
   * </ul>
   */
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    ValueCache.CachedValue cachedValue = valueCache.get(eqPredicate, dataSourceMetadata.getDataType());
    // Check min/max value
    if (!checkMinMaxRange(dataSourceMetadata, cachedValue.getComparableValue())) {
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

  /**
   * For IN predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   * </ul>
   * <p>NOTE: segments will not be pruned if the number of values is greater than the threshold.
   */
  private boolean pruneInPredicate(IndexSegment segment, InPredicate inPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    List<String> values = inPredicate.getValues();
    // Skip pruning when there are too many values in the IN predicate
    if (values.size() > _inPredicateThreshold) {
      return false;
    }
    String column = inPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    List<ValueCache.CachedValue> cachedValues = valueCache.get(inPredicate, dataSourceMetadata.getDataType());
    // Check min/max value
    for (ValueCache.CachedValue value : cachedValues) {
      if (checkMinMaxRange(dataSourceMetadata, value.getComparableValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   * For RANGE predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   * </ul>
   */
  private boolean pruneRangePredicate(IndexSegment segment, RangePredicate rangePredicate,
      Map<String, DataSource> dataSourceCache) {
    String column = rangePredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();

    // Get lower/upper boundary value
    DataType dataType = dataSourceMetadata.getDataType();
    String lowerBound = rangePredicate.getLowerBound();
    Comparable lowerBoundValue = null;
    if (!lowerBound.equals(RangePredicate.UNBOUNDED)) {
      lowerBoundValue = convertValue(lowerBound, dataType);
    }
    boolean lowerInclusive = rangePredicate.isLowerInclusive();
    String upperBound = rangePredicate.getUpperBound();
    Comparable upperBoundValue = null;
    if (!upperBound.equals(RangePredicate.UNBOUNDED)) {
      upperBoundValue = convertValue(upperBound, dataType);
    }
    boolean upperInclusive = rangePredicate.isUpperInclusive();

    // Check if the range is valid
    // TODO: This check should be performed on the broker
    if (lowerBoundValue != null && upperBoundValue != null) {
      if (lowerInclusive && upperInclusive) {
        if (lowerBoundValue.compareTo(upperBoundValue) > 0) {
          return true;
        }
      } else {
        if (lowerBoundValue.compareTo(upperBoundValue) >= 0) {
          return true;
        }
      }
    }

    // Check min/max value
    Comparable minValue = dataSourceMetadata.getMinValue();
    if (minValue != null) {
      if (upperBoundValue != null) {
        if (upperInclusive) {
          if (upperBoundValue.compareTo(minValue) < 0) {
            return true;
          }
        } else {
          if (upperBoundValue.compareTo(minValue) <= 0) {
            return true;
          }
        }
      }
    }
    Comparable maxValue = dataSourceMetadata.getMaxValue();
    if (maxValue != null) {
      if (lowerBoundValue != null) {
        if (lowerInclusive) {
          if (lowerBoundValue.compareTo(maxValue) > 0) {
            return true;
          }
        } else {
          if (lowerBoundValue.compareTo(maxValue) >= 0) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if the value is within the column's min/max value range, {@code false} otherwise.
   */
  private boolean checkMinMaxRange(DataSourceMetadata dataSourceMetadata, Comparable value) {
    Comparable minValue = dataSourceMetadata.getMinValue();
    if (minValue != null) {
      if (value.compareTo(minValue) < 0) {
        return false;
      }
    }
    Comparable maxValue = dataSourceMetadata.getMaxValue();
    if (maxValue != null) {
      if (value.compareTo(maxValue) > 0) {
        return false;
      }
    }
    return true;
  }
}
