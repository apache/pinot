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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountHLLPlusAggregationFunction extends BaseSingleInputAggregationFunction<HyperLogLogPlus, Long> {
  // The parameter "p" determines the precision of the sparse list in HyperLogLogPlus.
  protected final int _p;
  // The "sp" parameter specifies the number of standard deviations that the sparse list's precision should be set to.
  protected final int _sp;

  public DistinctCountHLLPlusAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 2 or 3 arguments.
    Preconditions.checkArgument(numExpressions <= 3, "DistinctCountHLLPlus expects 2 or 3 arguments, got: %s",
        numExpressions);
    if (arguments.size() == 2) {
      _p = arguments.get(1).getLiteral().getIntValue();
      _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;
    } else if (arguments.size() == 3) {
      _p = arguments.get(1).getLiteral().getIntValue();
      _sp = arguments.get(2).getLiteral().getIntValue();
    } else {
      _p = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P;
      _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;
    }
  }

  public int getP() {
    return _p;
  }

  public int getSp() {
    return _sp;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLLPLUS;
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized HyperLogLogPlus
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
        if (hyperLogLogPlus != null) {
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLogPlus = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(hyperLogLogPlus);
          for (int i = 1; i < length; i++) {
            hyperLogLogPlus.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLogPlus.offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLogPlus.offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLogPlus.offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLogPlus.offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLogPlus.offer(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized HyperLogLogPlus
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          HyperLogLogPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]);
          int groupKey = groupKeyArray[i];
          HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
          if (hyperLogLogPlus != null) {
            hyperLogLogPlus.addAll(value);
          } else {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized HyperLogLogPlus
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          HyperLogLogPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
            if (hyperLogLogPlus != null) {
              hyperLogLogPlus.addAll(value);
            } else {
              // Create a new HyperLogLogPlus for the group
              groupByResultHolder.setValueForKey(groupKey,
                  ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]));
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public HyperLogLogPlus extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new HyperLogLogPlus(_p, _sp);
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to HyperLogLogPlus
      return convertToHyperLogLogPlus((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the HyperLogLogPlus
      return (HyperLogLogPlus) result;
    }
  }

  @Override
  public HyperLogLogPlus extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new HyperLogLogPlus(_p, _sp);
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to HyperLogLogPlus
      return convertToHyperLogLogPlus((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the HyperLogLogPlus
      return (HyperLogLogPlus) result;
    }
  }

  @Override
  public HyperLogLogPlus merge(HyperLogLogPlus intermediateResult1, HyperLogLogPlus intermediateResult2) {
    // Can happen when aggregating serialized HyperLogLogPlus with non-default p, sp values
    if (intermediateResult1.sizeof() != intermediateResult2.sizeof()) {
      if (intermediateResult1.cardinality() == 0) {
        return intermediateResult2;
      } else {
        Preconditions.checkState(intermediateResult2.cardinality() == 0,
            "Cannot merge HyperLogLogPlus of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
    }
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(HyperLogLogPlus intermediateResult) {
    return intermediateResult.cardinality();
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the HyperLogLogPlus from the result holder or creates a new one if it does not exist.
   */
  protected HyperLogLogPlus getHyperLogLogPlus(AggregationResultHolder aggregationResultHolder) {
    HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_p, _sp);
      aggregationResultHolder.setValue(hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the HyperLogLogPlus for the given group key or creates a new one if it does not exist.
   */
  protected HyperLogLogPlus getHyperLogLogPlus(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_p, _sp);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /**
   * Helper method to set value for the given group keys into the result holder.
   */
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, Object value) {
    for (int groupKey : groupKeys) {
      getHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to HyperLogLogPlus for dictionary-encoded expression.
   */
  private HyperLogLogPlus convertToHyperLogLogPlus(DictIdsWrapper dictIdsWrapper) {
    HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(_p, _sp);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      hyperLogLogPlus.offer(dictionary.get(iterator.next()));
    }
    return hyperLogLogPlus;
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }
}
