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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountHLLAggregationFunction extends BaseSingleInputAggregationFunction<HyperLogLog, Long> {
  // When the dictionary size exceeds this threshold, dictionary IDs are offered directly to HyperLogLog
  // rather than being collected in a RoaringBitmap for deduplication first. For high-cardinality columns,
  // this avoids the O(n log n) cost of bitmap insertions and provides significant speedup.
  //
  // 100K is chosen as the crossover point where direct-HLL becomes faster than bitmap dedup:
  // - Below 100K: RoaringBitmap is compact (~12KB), insertions are cheap, and pre-deduplication
  //   marginally improves HLL accuracy by reducing duplicate offers before finalization.
  // - Above 100K: bitmap memory and insertion cost dominate; HLL's ~0.8% error (log2m=12)
  //   makes exact pre-deduplication negligible for correctness anyway.
  // This default matches DISTINCT_COUNT_SMART_HLL's dictThreshold default (see #17411).
  public static final int DEFAULT_DICT_SIZE_THRESHOLD = 100_000;

  protected final int _log2m;
  protected final int _dictSizeThreshold;

  public DistinctCountHLLAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1, 2, or 3 arguments.
    Preconditions.checkArgument(numExpressions <= 3, "DistinctCountHLL expects 1, 2, or 3 arguments, got: %s",
        numExpressions);
    if (numExpressions >= 2) {
      _log2m = arguments.get(1).getLiteral().getIntValue();
    } else {
      _log2m = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M;
    }
    if (numExpressions >= 3) {
      int dictSizeThreshold = arguments.get(2).getLiteral().getIntValue();
      _dictSizeThreshold = dictSizeThreshold > 0 ? dictSizeThreshold : Integer.MAX_VALUE;
    } else {
      _dictSizeThreshold = DEFAULT_DICT_SIZE_THRESHOLD;
    }
  }

  public int getLog2m() {
    return _log2m;
  }

  public int getDictSizeThreshold() {
    return _dictSizeThreshold;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLL;
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

    // Treat BYTES value as serialized HyperLogLog
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
        if (hyperLogLog != null) {
          for (int i = 0; i < length; i++) {
            hyperLogLog.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLog = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(hyperLogLog);
          for (int i = 1; i < length; i++) {
            hyperLogLog.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
      return;
    }

    if (blockValSet.isSingleValue()) {
      aggregateSV(length, aggregationResultHolder, blockValSet, storedType);
    } else {
      aggregateMV(length, aggregationResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (dictionary.length() > _dictSizeThreshold) {
        // High-cardinality dictionary: bypass RoaringBitmap and offer values directly to HLL.
        // Avoids O(n log n) bitmap insertion cost at the expense of approximate deduplication,
        // which is acceptable since DISTINCTCOUNTHLL already returns an approximate result.
        HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(dictionary.get(dictIds[i]));
        }
      } else {
        getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
    HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          hyperLogLog.offer(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  protected void aggregateMV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      if (dictionary.length() > _dictSizeThreshold) {
        HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);
        for (int i = 0; i < length; i++) {
          for (int dictId : dictIds[i]) {
            hyperLogLog.offer(dictionary.get(dictId));
          }
        }
      } else {
        RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
        for (int i = 0; i < length; i++) {
          dictIdBitmap.add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
    HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (String value : stringValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized HyperLogLog
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          HyperLogLog value = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]);
          int groupKey = groupKeyArray[i];
          HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
          if (hyperLogLog != null) {
            hyperLogLog.addAll(value);
          } else {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
      return;
    }

    if (blockValSet.isSingleValue()) {
      aggregateSVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet, storedType);
    } else {
      aggregateMVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (dictionary.length() > _dictSizeThreshold) {
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(dictionary.get(dictIds[i]));
        }
      } else {
        for (int i = 0; i < length; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  protected void aggregateMVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      if (dictionary.length() > _dictSizeThreshold) {
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (int dictId : dictIds[i]) {
            hyperLogLog.offer(dictionary.get(dictId));
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (int value : intValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (long value : longValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (float value : floatValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (double value : doubleValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
          for (String value : stringValuesArray[i]) {
            hyperLogLog.offer(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized HyperLogLog
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          HyperLogLog value = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
            if (hyperLogLog != null) {
              hyperLogLog.addAll(value);
            } else {
              // Create a new HyperLogLog for the group
              groupByResultHolder.setValueForKey(groupKey,
                  ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
      return;
    }

    if (blockValSet.isSingleValue()) {
      aggregateSVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet, storedType);
    } else {
      aggregateMVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (dictionary.length() > _dictSizeThreshold) {
        for (int i = 0; i < length; i++) {
          Object value = dictionary.get(dictIds[i]);
          for (int groupKey : groupKeysArray[i]) {
            getHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
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
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  protected void aggregateMVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, use adaptive strategy based on dictionary size
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      if (dictionary.length() > _dictSizeThreshold) {
        for (int i = 0; i < length; i++) {
          int[] rowDictIds = dictIds[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (int dictId : rowDictIds) {
              hyperLogLog.offer(dictionary.get(dictId));
            }
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
          }
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLog
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          int[] intValues = intValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (int value : intValues) {
              hyperLogLog.offer(value);
            }
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          long[] longValues = longValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (long value : longValues) {
              hyperLogLog.offer(value);
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          float[] floatValues = floatValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (float value : floatValues) {
              hyperLogLog.offer(value);
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          double[] doubleValues = doubleValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (double value : doubleValues) {
              hyperLogLog.offer(value);
            }
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          String[] stringValues = stringValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
            for (String value : stringValues) {
              hyperLogLog.offer(value);
            }
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + storedType);
    }
  }

  @Override
  public HyperLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new HyperLogLog(_log2m);
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to HyperLogLog
      return convertToHyperLogLog((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the HyperLogLog
      return (HyperLogLog) result;
    }
  }

  @Override
  public HyperLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new HyperLogLog(_log2m);
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to HyperLogLog
      return convertToHyperLogLog((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the HyperLogLog
      return (HyperLogLog) result;
    }
  }

  @Override
  public HyperLogLog merge(HyperLogLog intermediateResult1, HyperLogLog intermediateResult2) {
    // Can happen when aggregating serialized HyperLogLog with non-default log2m
    if (intermediateResult1.sizeof() != intermediateResult2.sizeof()) {
      if (intermediateResult1.cardinality() == 0) {
        return intermediateResult2;
      } else {
        Preconditions.checkState(intermediateResult2.cardinality() == 0,
            "Cannot merge HyperLogLogs of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
    }
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(HyperLogLog hyperLogLog) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.HyperLogLog.getValue(),
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog));
  }

  @Override
  public HyperLogLog deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Nullable
  @Override
  public Long extractFinalResult(@Nullable HyperLogLog intermediateResult) {
    return intermediateResult == null ? 0L : intermediateResult.cardinality();
  }

  @Override
  public Long mergeFinalResult(Long finalResult1, Long finalResult2) {
    return finalResult1 + finalResult2;
  }

  @Override
  public boolean canUseStarTree(Map<String, Object> functionParameters) {
    // Check if log2m matches
    Object log2m = functionParameters.get(Constants.HLL_LOG2M_KEY);
    if (log2m != null) {
      return _log2m == Integer.parseInt(String.valueOf(log2m));
    } else {
      // If the functionParameters don't have an explicit log2m set, it means that the star-tree index was built with
      // the default value for log2m
      return _log2m == CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M;
    }
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
   * Returns the HyperLogLog from the result holder or creates a new one if it does not exist.
   */
  protected HyperLogLog getHyperLogLog(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(_log2m);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
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
   * Returns the HyperLogLog for the given group key or creates a new one if it does not exist.
   */
  protected HyperLogLog getHyperLogLog(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(_log2m);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
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
      getHyperLogLog(groupByResultHolder, groupKey).offer(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to HyperLogLog for dictionary-encoded expression.
   */
  private HyperLogLog convertToHyperLogLog(DictIdsWrapper dictIdsWrapper) {
    HyperLogLog hyperLogLog = new HyperLogLog(_log2m);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      hyperLogLog.offer(dictionary.get(iterator.next()));
    }
    return hyperLogLog;
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
