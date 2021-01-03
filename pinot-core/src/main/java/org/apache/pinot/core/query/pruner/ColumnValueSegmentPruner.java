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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The {@code ColumnValueSegmentPruner} is the segment pruner that prunes segments based on the value inside the filter.
 * <ul>
 *   <li>
 *     For EQUALITY filter, prune the segment based on:
 *     <ul>
 *       <li>Column min/max value</li>
 *       <li>Column partition</li>
 *       <li>Column bloom filter</li>
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
@SuppressWarnings({"rawtypes", "unchecked"})
public class ColumnValueSegmentPruner implements SegmentPruner {

  @Override
  public void init(PinotConfiguration config) {
  }

  @Override
  public boolean prune(IndexSegment segment, QueryContext query) {
    FilterContext filter = query.getFilter();
    if (filter == null) {
      return false;
    }

    // This map caches the data sources
    Map<String, DataSource> dataSourceCache = new HashMap<>();
    return pruneSegment(segment, filter, dataSourceCache);
  }

  private boolean pruneSegment(IndexSegment segment, FilterContext filter, Map<String, DataSource> dataSourceCache) {
    switch (filter.getType()) {
      case AND:
        for (FilterContext child : filter.getChildren()) {
          if (pruneSegment(segment, child, dataSourceCache)) {
            return true;
          }
        }
        return false;
      case OR:
        for (FilterContext child : filter.getChildren()) {
          if (!pruneSegment(segment, child, dataSourceCache)) {
            return false;
          }
        }
        return true;
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        // Only prune columns
        if (predicate.getLhs().getType() != ExpressionContext.Type.IDENTIFIER) {
          return false;
        }
        Predicate.Type predicateType = predicate.getType();
        if (predicateType == Predicate.Type.EQ) {
          return pruneEqPredicate(segment, (EqPredicate) predicate, dataSourceCache);
        } else if (predicateType == Predicate.Type.RANGE) {
          return pruneRangePredicate(segment, (RangePredicate) predicate, dataSourceCache);
        } else {
          return false;
        }
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * For EQ predicate, prune the segment based on:
   * <ul>
   *   <li>Column min/max value</li>
   *   <li>Column partition</li>
   *   <li>Column bloom filter</li>
   * </ul>
   */
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Comparable value = convertValue(eqPredicate.getValue(), dataSourceMetadata.getDataType());

    // Check min/max value
    Comparable minValue = dataSourceMetadata.getMinValue();
    if (minValue != null) {
      if (value.compareTo(minValue) < 0) {
        return true;
      }
    }
    Comparable maxValue = dataSourceMetadata.getMaxValue();
    if (maxValue != null) {
      if (value.compareTo(maxValue) > 0) {
        return true;
      }
    }

    // Check column partition
    PartitionFunction partitionFunction = dataSourceMetadata.getPartitionFunction();
    if (partitionFunction != null) {
      Set<Integer> partitions = dataSourceMetadata.getPartitions();
      assert partitions != null;
      if (!partitions.contains(partitionFunction.getPartition(value))) {
        return true;
      }
    }

    // Check bloom filter
    BloomFilterReader bloomFilter = dataSource.getBloomFilter();
    if (bloomFilter != null) {
      if (!bloomFilter.mightContain(value.toString())) {
        return true;
      }
    }

    return false;
  }

  /**
   * For RANGE predicate, prune the segment based on:
   * <ul>
   *   <li>Column min/max value</li>
   * </ul>
   */
  private boolean pruneRangePredicate(IndexSegment segment, RangePredicate rangePredicate,
      Map<String, DataSource> dataSourceCache) {
    String column = rangePredicate.getLhs().getIdentifier();
    DataSource dataSource = dataSourceCache.computeIfAbsent(column, segment::getDataSource);
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
   * Convert String value to specified numerical type. We first verify that the input string contains a number by parsing
   * it as Double. The resulting Double is then downcast to specified numerical type. This allows us to create predicates
   * which allow for comparing values of two different numerical types such as:
   * SELECT * FROM table WHERE a > 5.0
   * SELECT * FROM table WHERE timestamp > NOW() - 5.0.
   */
  private static Comparable convertValue(String stringValue, DataType dataType) {
    try {
      switch (dataType) {
        case INT:
          return Double.valueOf(stringValue).intValue();
        case LONG:
          return Double.valueOf(stringValue).longValue();
        case FLOAT:
          return Double.valueOf(stringValue).floatValue();
        case DOUBLE:
          return Double.valueOf(stringValue);
        case STRING:
          return stringValue;
        case BYTES:
          return BytesUtils.toByteArray(stringValue);
        default:
          throw new IllegalStateException();
      }
    } catch (Exception e) {
      throw new BadQueryRequestException(String.format("Cannot convert value: '%s' to type: %s", stringValue, dataType),
          e);
    }
  }
}
