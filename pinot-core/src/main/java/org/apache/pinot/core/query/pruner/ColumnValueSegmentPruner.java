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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.index.readers.bloom.GuavaBloomFilterReaderUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants.Server;


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
@SuppressWarnings({"rawtypes", "unchecked", "RedundantIfStatement"})
public class ColumnValueSegmentPruner implements SegmentPruner {

  public static final String IN_PREDICATE_THRESHOLD = "inpredicate.threshold";

  private int _inPredicateThreshold;
  private FetchPlanner _fetchPlanner;

  @Override
  public void init(PinotConfiguration config) {
    _inPredicateThreshold =
        config.getProperty(IN_PREDICATE_THRESHOLD, Server.DEFAULT_VALUE_PRUNER_IN_PREDICATE_THRESHOLD);
    _fetchPlanner = FetchPlannerRegistry.getPlanner();
  }

  @Override
  public boolean isApplicableTo(QueryContext query) {
    return query.getFilter() != null;
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }
    FilterContext filter = Objects.requireNonNull(query.getFilter());
    ValueCache cachedValues = new ValueCache();
    int numSegments = segments.size();
    List<IndexSegment> selectedSegments = new ArrayList<>(numSegments);
    if (query.isEnablePrefetch()) {
      FetchContext[] fetchContexts = new FetchContext[numSegments];
      try {
        // Prefetch bloom filter for columns within the EQ/IN predicate if exists
        for (int i = 0; i < numSegments; i++) {
          IndexSegment segment = segments.get(i);
          FetchContext fetchContext = _fetchPlanner.planFetchForPruning(segment, query);
          if (!fetchContext.isEmpty()) {
            segment.prefetch(fetchContext);
            fetchContexts[i] = fetchContext;
          }
        }
        // Prune segments
        Map[] dataSourceCaches = new Map[numSegments];
        for (int i = 0; i < numSegments; i++) {
          dataSourceCaches[i] = new HashMap<>();
          IndexSegment segment = segments.get(i);
          FetchContext fetchContext = fetchContexts[i];
          if (fetchContext != null) {
            segment.acquire(fetchContext);
            try {
              if (!pruneSegment(segment, filter, dataSourceCaches[i], cachedValues)) {
                selectedSegments.add(segment);
              }
            } finally {
              segment.release(fetchContext);
            }
          } else {
            if (!pruneSegment(segment, filter, dataSourceCaches[i], cachedValues)) {
              selectedSegments.add(segment);
            }
          }
        }
      } finally {
        // Release the prefetched bloom filters
        for (int i = 0; i < numSegments; i++) {
          FetchContext fetchContext = fetchContexts[i];
          if (fetchContext != null) {
            segments.get(i).release(fetchContext);
          }
        }
      }
    } else {
      Map<String, DataSource> dataSourceCache = new HashMap<>();
      for (IndexSegment segment : segments) {
        dataSourceCache.clear();
        if (!pruneSegment(segment, filter, dataSourceCache, cachedValues)) {
          selectedSegments.add(segment);
        }
      }
    }
    return selectedSegments;
  }

  private boolean pruneSegment(IndexSegment segment, FilterContext filter, Map<String, DataSource> dataSourceCache,
      ValueCache cachedValues) {
    switch (filter.getType()) {
      case AND:
        for (FilterContext child : filter.getChildren()) {
          if (pruneSegment(segment, child, dataSourceCache, cachedValues)) {
            return true;
          }
        }
        return false;
      case OR:
        for (FilterContext child : filter.getChildren()) {
          if (!pruneSegment(segment, child, dataSourceCache, cachedValues)) {
            return false;
          }
        }
        return true;
      case NOT:
        // Do not prune NOT filter
        return false;
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        // Only prune columns
        if (predicate.getLhs().getType() != ExpressionContext.Type.IDENTIFIER) {
          return false;
        }
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
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * For EQ predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   *   <li>Column partition</li>
   *   <li>Column bloom filter</li>
   * </ul>
   */
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment
        ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    ValueCache.CachedValue cachedValue = valueCache.get(eqPredicate, dataSourceMetadata.getDataType());

    Comparable value = cachedValue.getComparableValue();

    // Check min/max value
    if (!checkMinMaxRange(dataSourceMetadata, value)) {
      return true;
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
      if (!cachedValue.mightBeContained(bloomFilter)) {
        return true;
      }
    }

    return false;
  }

  /**
   * For IN predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   *   <li>Column bloom filter</li>
   * </ul>
   * <p>NOTE: segments will not be pruned if the number of values is greater than the threshold.
   */
  private boolean pruneInPredicate(IndexSegment segment, InPredicate inPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    String column = inPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment
        ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    List<String> values = inPredicate.getValues();

    // Skip pruning when there are too many values in the IN predicate
    if (values.size() > _inPredicateThreshold) {
      return false;
    }

    List<ValueCache.CachedValue> cachedValues = valueCache.get(inPredicate, dataSourceMetadata.getDataType());

    // Check min/max value
    boolean someInRange = false;
    for (ValueCache.CachedValue value : cachedValues) {
      if (checkMinMaxRange(dataSourceMetadata, value.getComparableValue())) {
        someInRange = true;
        break;
      }
    }
    if (!someInRange) {
      return true;
    }

    // Check bloom filter
    BloomFilterReader bloomFilter = dataSource.getBloomFilter();
    if (bloomFilter == null) {
      return false;
    }
    for (ValueCache.CachedValue value : cachedValues) {
      if (value.mightBeContained(bloomFilter)) {
        return false;
      }
    }
    return true;
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

  /**
   * For RANGE predicate, prune the segments based on:
   * <ul>
   *   <li>Column min/max value</li>
   * </ul>
   */
  private boolean pruneRangePredicate(IndexSegment segment, RangePredicate rangePredicate,
      Map<String, DataSource> dataSourceCache) {
    String column = rangePredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment
        ? segment.getDataSource(column)
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

  private static Comparable convertValue(String stringValue, DataType dataType) {
    try {
      return dataType.convertInternal(stringValue);
    } catch (Exception e) {
      throw new BadQueryRequestException(e);
    }
  }

  private static class ValueCache {
    // As Predicates are recursive structures, their hashCode is quite expensive.
    // By using an IdentityHashMap here we don't need to iterate over the recursive
    // structure. This is specially useful in the IN expression.
    private final Map<Predicate, Object> _cache = new IdentityHashMap<>();

    private CachedValue add(EqPredicate pred) {
      CachedValue val = new CachedValue(pred.getValue());
      _cache.put(pred, val);
      return val;
    }

    private List<CachedValue> add(InPredicate pred) {
      List<CachedValue> vals = new ArrayList<>(pred.getValues().size());
      for (String value : pred.getValues()) {
        vals.add(new CachedValue(value));
      }
      _cache.put(pred, vals);
      return vals;
    }

    public CachedValue get(EqPredicate pred, DataType dt) {
      CachedValue cachedValue = (CachedValue) _cache.get(pred);
      if (cachedValue == null) {
        cachedValue = add(pred);
      }
      cachedValue.ensureDataType(dt);
      return cachedValue;
    }

    public List<CachedValue> get(InPredicate pred, DataType dt) {
      List<CachedValue> cachedValues = (List<CachedValue>) _cache.get(pred);
      if (cachedValues == null) {
        cachedValues = add(pred);
      }
      for (CachedValue cachedValue : cachedValues) {
        cachedValue.ensureDataType(dt);
      }
      return cachedValues;
    }

    public static class CachedValue {
      private final Object _value;
      private boolean _hashed = false;
      private long _hash1;
      private long _hash2;
      private DataType _dt;
      private Comparable _comparableValue;

      private CachedValue(Object value) {
        _value = value;
      }

      private Comparable getComparableValue() {
        assert _dt != null;
        return _comparableValue;
      }

      private void ensureDataType(DataType dt) {
        if (dt != _dt) {
          String strValue = _value.toString();
          _dt = dt;
          _comparableValue = convertValue(strValue, dt);
          _hashed = false;
        }
      }

      private boolean mightBeContained(BloomFilterReader bloomFilter) {
        if (!_hashed) {
          GuavaBloomFilterReaderUtils.Hash128AsLongs hash128AsLongs =
              GuavaBloomFilterReaderUtils.hashAsLongs(_comparableValue.toString());
          _hash1 = hash128AsLongs.getHash1();
          _hash2 = hash128AsLongs.getHash2();
          _hashed = true;
        }
        return bloomFilter.mightContain(_hash1, _hash2);
      }
    }
  }
}
