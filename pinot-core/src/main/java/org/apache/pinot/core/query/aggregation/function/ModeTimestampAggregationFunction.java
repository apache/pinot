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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMaps;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/// Timestamp implementation of MODE, preserving epoch milliseconds without conversion through DOUBLE.
/// Instances are immutable; per-segment frequency maps are stored in the result holders.
public class ModeTimestampAggregationFunction extends BaseComparableModeAggregationFunction<Long> {
  public ModeTimestampAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled, "TIMESTAMP");
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MODETIMESTAMP;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.TIMESTAMP;
  }

  @Override
  protected Map<Long, Long> newValueMap() {
    return new Long2LongOpenHashMap();
  }

  @Override
  protected ValueCounter<Long> valueCounter(BlockValSet blockValSet) {
    long[] values = blockValSet.getLongValuesSV();
    return (counts, row) -> ((Long2LongOpenHashMap) counts).addTo(values[row], 1L);
  }

  @Override
  protected void putDictionaryCount(Map<Long, Long> counts, Dictionary dictionary, int dictionaryId, long count) {
    ((Long2LongOpenHashMap) counts).put(dictionary.getLongValue(dictionaryId), count);
  }

  @Override
  public Map<Long, Long> merge(Map<Long, Long> left, Map<Long, Long> right) {
    if (!(left instanceof Long2LongOpenHashMap) || !(right instanceof Long2LongMap)) {
      return super.merge(left, right);
    }
    Long2LongOpenHashMap counts = (Long2LongOpenHashMap) left;
    ObjectIterator<Long2LongMap.Entry> iterator = Long2LongMaps.fastIterator((Long2LongMap) right);
    while (iterator.hasNext()) {
      Long2LongMap.Entry entry = iterator.next();
      counts.addTo(entry.getLongKey(), entry.getLongValue());
    }
    return counts;
  }

  @Nullable
  @Override
  public Long extractFinalResult(@Nullable Map<Long, Long> counts) {
    if (!(counts instanceof Long2LongMap)) {
      return super.extractFinalResult(counts);
    }
    ObjectIterator<Long2LongMap.Entry> iterator = Long2LongMaps.fastIterator((Long2LongMap) counts);
    if (!iterator.hasNext()) {
      return null;
    }
    Long2LongMap.Entry first = iterator.next();
    long mode = first.getLongKey();
    long maxCount = first.getLongValue();
    while (iterator.hasNext()) {
      Long2LongMap.Entry entry = iterator.next();
      long value = entry.getLongKey();
      long count = entry.getLongValue();
      if (count > maxCount || (count == maxCount && (isMinimum() ? value < mode : value > mode))) {
        mode = value;
        maxCount = count;
      }
    }
    return mode;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Map<Long, Long> counts) {
    Long2LongMap longCounts = counts instanceof Long2LongMap ? (Long2LongMap) counts : new Long2LongOpenHashMap(counts);
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Long2LongMap.getValue(),
        ObjectSerDeUtils.LONG_2_LONG_MAP_SER_DE.serialize(longCounts));
  }

  @Override
  public Map<Long, Long> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.deserialize(customObject);
  }
}
