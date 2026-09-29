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


public class DistinctCountHLLPlusAggregationFunction extends BaseSingleInputAggregationFunction<HyperLogLogPlus, Long> {
  // The parameter "p" determines the precision of the sparse list in HyperLogLogPlus.
  protected final int _p;
  // The "sp" parameter specifies the number of standard deviations that the sparse list's precision should be set to.
  protected final int _sp;

  public DistinctCountHLLPlusAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
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

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized HyperLogLogPlus and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          int i = from;
          HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
          if (hyperLogLogPlus == null) {
            if (i == to) {
              return;
            }
            // The first HyperLogLogPlus read becomes the accumulator instead of being merged into a fresh one
            hyperLogLogPlus = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i++]);
            aggregationResultHolder.setValue(hyperLogLogPlus);
          }
          for (; i < to; i++) {
            hyperLogLogPlus.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
        }
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    if (singleValue) {
      aggregateSV(length, aggregationResultHolder, blockValSet, storedType);
    } else {
      aggregateMV(length, aggregationResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet,
          (from, to) -> getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, from, to - from));
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(stringValues[i]);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            hyperLogLogPlus.offer(bytesValues[i]);
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  protected void aggregateMV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
        for (int i = from; i < to; i++) {
          dictIdBitmap.add(dictIds[i]);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (int value : intValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (long value : longValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (float value : floatValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (double value : doubleValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (String value : stringValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValuesArray = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (byte[] value : bytesValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
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

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized HyperLogLogPlus and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          for (int i = from; i < to; i++) {
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
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    if (singleValue) {
      aggregateSVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet, storedType);
    } else {
      aggregateMVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(stringValues[i]);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(bytesValues[i]);
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  protected void aggregateMVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (int value : intValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (long value : longValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (float value : floatValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (double value : doubleValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (String value : stringValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValuesArray = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]);
            for (byte[] value : bytesValuesArray[i]) {
              hyperLogLogPlus.offer(value);
            }
          }
        });
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

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized HyperLogLogPlus and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          for (int i = from; i < to; i++) {
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
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    if (singleValue) {
      aggregateSVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet, storedType);
    } else {
      aggregateMVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet, storedType);
    }
  }

  protected void aggregateSVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], bytesValues[i]);
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS aggregation function: " + storedType);
    }
  }

  protected void aggregateMVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet, DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
          }
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the HyperLogLogPlus
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int[] intValues = intValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (int value : intValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            long[] longValues = longValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (long value : longValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            float[] floatValues = floatValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (float value : floatValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            double[] doubleValues = doubleValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (double value : doubleValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            String[] stringValues = stringValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (String value : stringValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValuesArray = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            byte[][] bytesValues = bytesValuesArray[i];
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = getHyperLogLogPlus(groupByResultHolder, groupKey);
              for (byte[] value : bytesValues) {
                hyperLogLogPlus.offer(value);
              }
            }
          }
        });
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
    // Cannot merge HyperLogLogPlus with different p values
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
  public SerializedIntermediateResult serializeIntermediateResult(HyperLogLogPlus hyperLogLogPlus) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.HyperLogLogPlus.getValue(),
        ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.serialize(hyperLogLogPlus));
  }

  @Override
  public HyperLogLogPlus deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(@Nullable HyperLogLogPlus intermediateResult) {
    return intermediateResult == null ? 0L : intermediateResult.cardinality();
  }

  @Override
  public Long mergeFinalResult(Long finalResult1, Long finalResult2) {
    return finalResult1 + finalResult2;
  }

  @Override
  public boolean canUseStarTree(Map<String, Object> functionParameters) {
    // Check if p value matches
    Object p = functionParameters.get(Constants.HLLPLUS_ULL_P_KEY);
    if (p != null) {
      return _p == Integer.parseInt(String.valueOf(p));
    } else {
      // If the functionParameters don't have an explicit p set, it means that the star-tree index was built with
      // the default value for p
      return _p == CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P;
    }
  }

  /// Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /// Returns the HyperLogLogPlus from the result holder or creates a new one if it does not exist.
  protected HyperLogLogPlus getHyperLogLogPlus(AggregationResultHolder aggregationResultHolder) {
    HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_p, _sp);
      aggregationResultHolder.setValue(hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }

  /// Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /// Returns the HyperLogLogPlus for the given group key or creates a new one if it does not exist.
  protected HyperLogLogPlus getHyperLogLogPlus(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_p, _sp);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }

  /// Helper method to set dictionary id for the given group keys into the result holder.
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /// Helper method to set value for the given group keys into the result holder.
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, Object value) {
    for (int groupKey : groupKeys) {
      getHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
    }
  }

  /// Helper method to read dictionary and convert dictionary ids to HyperLogLogPlus for dictionary-encoded expression.
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
