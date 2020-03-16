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
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
public class ColumnValueSegmentPruner implements SegmentPruner {

  @Override
  public void init(Configuration config) {
  }

  @Override
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    FilterQueryTree filterQueryTree = queryRequest.getFilterQueryTree();
    if (filterQueryTree == null) {
      return false;
    }

    // This map caches the data sources
    Map<String, DataSource> dataSourceCache = new HashMap<>();
    return pruneSegment(segment, filterQueryTree, dataSourceCache);
  }

  private boolean pruneSegment(IndexSegment segment, FilterQueryTree filterQueryTree,
      Map<String, DataSource> dataSourceCache) {
    // Only prune columns
    TransformExpressionTree expression = filterQueryTree.getExpression();
    if (expression != null && !expression.isColumn()) {
      return false;
    }

    switch (filterQueryTree.getOperator()) {
      case AND:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (pruneSegment(segment, child, dataSourceCache)) {
            return true;
          }
        }
        return false;
      case OR:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (!pruneSegment(segment, child, dataSourceCache)) {
            return false;
          }
        }
        return true;
      case EQUALITY:
        return pruneEqualityFilter(segment, filterQueryTree, dataSourceCache);
      case RANGE:
        return pruneRangeFilter(segment, filterQueryTree, dataSourceCache);
      default:
        return false;
    }
  }

  /**
   * For EQUALITY filter, prune the segment based on:
   * <ul>
   *   <li>Column min/max value</li>
   *   <li>Column partition</li>
   *   <li>Column bloom filter</li>
   * </ul>
   */
  @SuppressWarnings("unchecked")
  private boolean pruneEqualityFilter(IndexSegment segment, FilterQueryTree filterQueryTree,
      Map<String, DataSource> dataSourceCache) {
    String column = filterQueryTree.getColumn();
    DataSource dataSource = dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Comparable value = convertValue(filterQueryTree.getValue().get(0), dataSourceMetadata.getDataType());

    // Check min/max value
    Comparable minValue = dataSourceMetadata.getMinValue();
    if (minValue != null) {
      Comparable maxValue = dataSourceMetadata.getMaxValue();
      assert maxValue != null;
      if (value.compareTo(minValue) < 0 || value.compareTo(maxValue) > 0) {
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
      if (!bloomFilter.mightContain(value)) {
        return true;
      }
    }

    return false;
  }

  /**
   * For RANGE filter, prune the segment based on:
   * <ul>
   *   <li>Column min/max value</li>
   * </ul>
   */
  @SuppressWarnings("unchecked")
  private boolean pruneRangeFilter(IndexSegment segment, FilterQueryTree filterQueryTree,
      Map<String, DataSource> dataSourceCache) {
    String column = filterQueryTree.getColumn();
    DataSource dataSource = dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();

    // Get lower/upper boundary value
    DataType dataType = dataSourceMetadata.getDataType();
    RangePredicate rangePredicate = new RangePredicate(null, filterQueryTree.getValue());
    String lowerBoundary = rangePredicate.getLowerBoundary();
    boolean includeLowerBoundary = rangePredicate.includeLowerBoundary();
    Comparable lowerBoundaryValue = null;
    if (!lowerBoundary.equals(RangePredicate.UNBOUNDED)) {
      lowerBoundaryValue = convertValue(lowerBoundary, dataType);
    }
    String upperBoundary = rangePredicate.getUpperBoundary();
    boolean includeUpperBoundary = rangePredicate.includeUpperBoundary();
    Comparable upperBoundaryValue = null;
    if (!upperBoundary.equals(RangePredicate.UNBOUNDED)) {
      upperBoundaryValue = convertValue(upperBoundary, dataType);
    }

    // Check if the range is valid
    // TODO: This check should be performed on the broker
    if (lowerBoundaryValue != null && upperBoundaryValue != null) {
      if (includeLowerBoundary && includeUpperBoundary) {
        if (lowerBoundaryValue.compareTo(upperBoundaryValue) > 0) {
          return true;
        }
      } else {
        if (lowerBoundaryValue.compareTo(upperBoundaryValue) >= 0) {
          return true;
        }
      }
    }

    // Check min/max value
    Comparable minValue = dataSourceMetadata.getMinValue();
    if (minValue != null) {
      Comparable maxValue = dataSourceMetadata.getMaxValue();
      assert maxValue != null;

      if (lowerBoundaryValue != null) {
        if (includeLowerBoundary) {
          if (lowerBoundaryValue.compareTo(maxValue) > 0) {
            return true;
          }
        } else {
          if (lowerBoundaryValue.compareTo(maxValue) >= 0) {
            return true;
          }
        }
      }
      if (upperBoundaryValue != null) {
        if (includeUpperBoundary) {
          if (upperBoundaryValue.compareTo(minValue) < 0) {
            return true;
          }
        } else {
          if (upperBoundaryValue.compareTo(minValue) <= 0) {
            return true;
          }
        }
      }
    }

    return false;
  }

  private static Comparable convertValue(String stringValue, DataType dataType) {
    try {
      switch (dataType) {
        case INT:
          return Integer.valueOf(stringValue);
        case LONG:
          return Long.valueOf(stringValue);
        case FLOAT:
          return Float.valueOf(stringValue);
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
