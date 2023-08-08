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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.index.readers.bloom.GuavaBloomFilterReaderUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants.Server;


/**
 * The {@code ValueBasedSegmentPruner} prunes segments based on values inside the filter and segment metadata and data.
 */
@SuppressWarnings({"rawtypes", "unchecked", "RedundantIfStatement"})
abstract public class ValueBasedSegmentPruner implements SegmentPruner {
  public static final String IN_PREDICATE_THRESHOLD = "inpredicate.threshold";
  protected int _inPredicateThreshold;

  @Override
  public void init(PinotConfiguration config) {
    _inPredicateThreshold =
        config.getProperty(IN_PREDICATE_THRESHOLD, Server.DEFAULT_VALUE_PRUNER_IN_PREDICATE_THRESHOLD);
  }

  @Override
  public boolean isApplicableTo(QueryContext query) {
    if (query.getFilter() == null) {
      return false;
    }
    return isApplicableToFilter(query.getFilter());
  }

  /**
   * 1. NOT is not applicable for segment pruning;
   * 2. For OR, if one of the child filter is not applicable for pruning, the parent filter is not applicable;
   * 3. For AND, if one of the child filter is applicable for pruning, the parent filter is applicable, but it
   *    doesn't mean this child filter can prune the segment.
   * 4. The specific pruners decide their own applicable predicate types.
   */
  private boolean isApplicableToFilter(FilterContext filter) {
    switch (filter.getType()) {
      case AND:
        for (FilterContext child : filter.getChildren()) {
          if (isApplicableToFilter(child)) {
            return true;
          }
        }
        return false;
      case OR:
        for (FilterContext child : filter.getChildren()) {
          if (!isApplicableToFilter(child)) {
            return false;
          }
        }
        return true;
      case NOT:
        // Do not prune NOT filter
        return false;
      case PREDICATE:
        return isApplicableToPredicate(filter.getPredicate());
      default:
        throw new IllegalStateException();
    }
  }

  abstract boolean isApplicableToPredicate(Predicate predicate);

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }
    FilterContext filter = Objects.requireNonNull(query.getFilter());
    ValueCache cachedValues = new ValueCache();
    Map<String, DataSource> dataSourceCache = new HashMap<>();
    List<IndexSegment> selectedSegments = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      dataSourceCache.clear();
      if (!pruneSegment(segment, filter, dataSourceCache, cachedValues)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  protected boolean pruneSegment(IndexSegment segment, FilterContext filter, Map<String, DataSource> dataSourceCache,
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
        return pruneSegmentWithPredicate(segment, predicate, dataSourceCache, cachedValues);
      default:
        throw new IllegalStateException();
    }
  }

  abstract boolean pruneSegmentWithPredicate(IndexSegment segment, Predicate predicate,
      Map<String, DataSource> dataSourceCache, ValueCache cachedValues);

  protected static Comparable convertValue(String stringValue, DataType dataType) {
    try {
      return dataType.convertInternal(stringValue);
    } catch (Exception e) {
      throw new BadQueryRequestException(e);
    }
  }

  protected static class ValueCache {
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

      public Comparable getComparableValue() {
        assert _dt != null;
        return _comparableValue;
      }

      public void ensureDataType(DataType dt) {
        if (dt != _dt) {
          String strValue = _value.toString();
          _dt = dt;
          _comparableValue = convertValue(strValue, dt);
          _hashed = false;
        }
      }

      public boolean mightBeContained(BloomFilterReader bloomFilter) {
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
