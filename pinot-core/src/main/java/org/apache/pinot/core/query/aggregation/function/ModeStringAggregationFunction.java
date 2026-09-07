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

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
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


/// String implementation of MODE, with lexicographic MIN/MAX tie resolution and a fixed STRING result type.
/// Instances are immutable; per-segment frequency maps are stored in the result holders.
public class ModeStringAggregationFunction extends BaseComparableModeAggregationFunction<String> {
  public ModeStringAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled, "STRING");
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MODESTRING;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  protected Map<String, Long> newValueMap() {
    return new StringModeCounts();
  }

  @Override
  protected ValueCounter<String> valueCounter(BlockValSet blockValSet) {
    String[] values = blockValSet.getStringValuesSV();
    return (counts, row) -> ((StringModeCounts) counts).addTo(values[row], 1L);
  }

  @Override
  protected void putDictionaryCount(Map<String, Long> counts, Dictionary dictionary, int dictionaryId, long count) {
    ((StringModeCounts) counts).put(dictionary.getStringValue(dictionaryId), count);
  }

  @Override
  public Map<String, Long> merge(Map<String, Long> left, Map<String, Long> right) {
    if (left instanceof Object2LongOpenHashMap && right instanceof Object2LongMap) {
      Object2LongOpenHashMap<String> counts = (Object2LongOpenHashMap<String>) left;
      ObjectIterator<Object2LongMap.Entry<String>> iterator =
          Object2LongMaps.fastIterator((Object2LongMap<String>) right);
      while (iterator.hasNext()) {
        Object2LongMap.Entry<String> entry = iterator.next();
        counts.addTo(entry.getKey(), entry.getLongValue());
      }
      return left;
    }
    return super.merge(left, right);
  }

  @Nullable
  @Override
  public String extractFinalResult(@Nullable Map<String, Long> counts) {
    if (!(counts instanceof Object2LongMap)) {
      return super.extractFinalResult(counts);
    }
    String mode = null;
    long maxCount = 0;
    ObjectIterator<Object2LongMap.Entry<String>> iterator =
        Object2LongMaps.fastIterator((Object2LongMap<String>) counts);
    while (iterator.hasNext()) {
      Object2LongMap.Entry<String> entry = iterator.next();
      String value = entry.getKey();
      long count = entry.getLongValue();
      if (mode == null || count > maxCount || (count == maxCount
          && (isMinimum() ? value.compareTo(mode) < 0 : value.compareTo(mode) > 0))) {
        mode = value;
        maxCount = count;
      }
    }
    return mode;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
  public SerializedIntermediateResult serializeIntermediateResult(Map<String, Long> counts) {
    // Reuse the existing map wire encoding so generic aggregation bridges can deserialize the frequency state.
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Map.getValue(),
        ObjectSerDeUtils.MAP_SER_DE.serialize((Map) counts));
  }

  @Override
  public Map<String, Long> deserializeIntermediateResult(CustomObject customObject) {
    return new StringModeCounts(ObjectSerDeUtils.deserialize(customObject));
  }

  /// Frequency state with an O(1) conservative estimate of the retained string-key payload.
  /// Accumulation uses [#addTo] and dictionary extraction and boxed [Map#merge] use [#put].
  /// Each distinct key is charged once, assuming UTF-16 storage plus object and array overhead.
  /// Instances belong to one result holder and are not thread-safe.
  public static final class StringModeCounts extends Object2LongOpenHashMap<String> {
    private long _retainedStringBytes;

    public StringModeCounts() {
    }

    /// Restores accounting once when a generic map is deserialized from the existing wire format.
    public StringModeCounts(Map<String, Long> counts) {
      super(counts.size());
      counts.forEach((value, count) -> put(value, count.longValue()));
    }

    public long getRetainedStringBytes() {
      return _retainedStringBytes;
    }

    @Override
    public long addTo(String value, long increment) {
      int previousSize = size();
      long previousCount = super.addTo(value, increment);
      if (size() != previousSize) {
        _retainedStringBytes += retainedStringBytes(value);
      }
      return previousCount;
    }

    @Override
    public long put(String value, long count) {
      int previousSize = size();
      long previousCount = super.put(value, count);
      if (size() != previousSize) {
        _retainedStringBytes += retainedStringBytes(value);
      }
      return previousCount;
    }

    @Override
    public long removeLong(Object value) {
      int previousSize = size();
      long previousCount = super.removeLong(value);
      if (size() != previousSize) {
        _retainedStringBytes -= retainedStringBytes((String) value);
      }
      return previousCount;
    }

    @Override
    public void clear() {
      super.clear();
      _retainedStringBytes = 0;
    }

    private static long retainedStringBytes(String value) {
      return 48 + 2L * value.length();
    }
  }
}
