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
package org.apache.pinot.core.query.aggregation.function;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;

import static com.google.common.base.Preconditions.checkArgument;


/// Counts comparable values and resolves equally frequent values using MIN (default) or MAX.
///
/// Instances are immutable and may be shared across segments. Accumulators belong to result holders, and dictionary
/// identifiers are converted to values before merging across segments.
/// Numeric MODE retains its existing implementation.
abstract class BaseComparableModeAggregationFunction<T extends Comparable<T>>
    extends BaseSingleInputAggregationFunction<Map<T, Long>, T> {
  private final boolean _minimum;

  protected BaseComparableModeAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled,
      String valueType) {
    super(checkArguments(arguments), nullHandlingEnabled);
    String reducer = "MIN";
    if (arguments.size() == 2) {
      ExpressionContext argument = arguments.get(1);
      checkArgument(argument.getType() == ExpressionContext.Type.LITERAL,
          "MODE tie reducer must be a literal MIN or MAX for %s", valueType);
      reducer = argument.getLiteral().getStringValue();
    }
    checkArgument("MIN".equals(reducer) || "MAX".equals(reducer),
        "MODE for %s supports only MIN or MAX tie reducers, got: %s", valueType, reducer);
    _minimum = "MIN".equals(reducer);
  }

  private static ExpressionContext checkArguments(List<ExpressionContext> arguments) {
    checkArgument(arguments.size() == 1 || arguments.size() == 2,
        "MODE expects one or two arguments, got: %s", arguments.size());
    return arguments.get(0);
  }

  protected abstract Map<T, Long> newValueMap();

  protected abstract ValueCounter<T> valueCounter(BlockValSet blockValSet);

  @FunctionalInterface
  protected interface ValueCounter<V> {
    void add(Map<V, Long> counts, int row);
  }

  protected abstract void putDictionaryCount(Map<T, Long> counts, Dictionary dictionary, int dictionaryId, long count);

  protected final boolean isMinimum() {
    return _minimum;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder holder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet values = blockValSetMap.get(_expression);
    Dictionary dictionary = values.isDictionaryEncoded() ? values.getDictionary() : null;
    if (dictionary != null) {
      int[] ids = values.getDictionaryIdsSV();
      forEachNotNull(length, values, (from, to) -> {
        DictionaryCounts counts = getValue(holder, () -> new DictionaryCounts(dictionary));
        for (int i = from; i < to; i++) {
          counts._counts.addTo(ids[i], 1L);
        }
      });
    } else {
      ValueCounter<T> counter = valueCounter(values);
      forEachNotNull(length, values, (from, to) -> {
        Map<T, Long> counts = getValue(holder, this::newValueMap);
        for (int i = from; i < to; i++) {
          counter.add(counts, i);
        }
      });
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder holder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet values = blockValSetMap.get(_expression);
    Dictionary dictionary = values.isDictionaryEncoded() ? values.getDictionary() : null;
    if (dictionary != null) {
      int[] ids = values.getDictionaryIdsSV();
      forEachNotNull(length, values, (from, to) -> {
        for (int i = from; i < to; i++) {
          DictionaryCounts counts = getValue(holder, groupKeys[i], () -> new DictionaryCounts(dictionary));
          counts._counts.addTo(ids[i], 1L);
        }
      });
    } else {
      ValueCounter<T> counter = valueCounter(values);
      forEachNotNull(length, values, (from, to) -> {
        for (int i = from; i < to; i++) {
          Map<T, Long> counts = getValue(holder, groupKeys[i], this::newValueMap);
          counter.add(counts, i);
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeys, GroupByResultHolder holder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet values = blockValSetMap.get(_expression);
    Dictionary dictionary = values.isDictionaryEncoded() ? values.getDictionary() : null;
    if (dictionary != null) {
      int[] ids = values.getDictionaryIdsSV();
      forEachNotNull(length, values, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeys[i]) {
            DictionaryCounts counts = getValue(holder, groupKey, () -> new DictionaryCounts(dictionary));
            counts._counts.addTo(ids[i], 1L);
          }
        }
      });
    } else {
      ValueCounter<T> counter = valueCounter(values);
      forEachNotNull(length, values, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeys[i]) {
            Map<T, Long> counts = getValue(holder, groupKey, this::newValueMap);
            counter.add(counts, i);
          }
        }
      });
    }
  }

  @Nullable
  @Override
  public Map<T, Long> extractAggregationResult(AggregationResultHolder holder) {
    return extractCounts(holder.getResult());
  }

  @Nullable
  @Override
  public Map<T, Long> extractGroupByResult(GroupByResultHolder holder, int groupKey) {
    return extractCounts(holder.getResult(groupKey));
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private Map<T, Long> extractCounts(@Nullable Object result) {
    if (result instanceof DictionaryCounts) {
      DictionaryCounts dictionaryCounts = (DictionaryCounts) result;
      Map<T, Long> counts = newValueMap();
      dictionaryCounts._counts.int2LongEntrySet().fastForEach(entry -> putDictionaryCount(
          counts, dictionaryCounts._dictionary, entry.getIntKey(), entry.getLongValue()));
      return counts;
    }
    return (Map<T, Long>) result;
  }

  @Override
  public Map<T, Long> merge(Map<T, Long> left, Map<T, Long> right) {
    right.forEach((value, count) -> left.merge(value, count, Long::sum));
    return left;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Nullable
  @Override
  public T extractFinalResult(@Nullable Map<T, Long> counts) {
    if (counts == null || counts.isEmpty()) {
      return null;
    }
    T mode = null;
    long maxCount = 0;
    for (Map.Entry<T, Long> entry : counts.entrySet()) {
      T value = entry.getKey();
      long count = entry.getValue();
      if (mode == null || count > maxCount || (count == maxCount
          && (_minimum ? value.compareTo(mode) < 0 : value.compareTo(mode) > 0))) {
        mode = value;
        maxCount = count;
      }
    }
    return mode;
  }

  private static final class DictionaryCounts {
    private final Dictionary _dictionary;
    private final Int2LongOpenHashMap _counts = new Int2LongOpenHashMap();

    private DictionaryCounts(Dictionary dictionary) {
      _dictionary = dictionary;
    }
  }
}
