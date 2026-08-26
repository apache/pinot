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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
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
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountULLAggregationFunction extends BaseSingleInputAggregationFunction<UltraLogLog, Comparable> {
  protected final int _p;

  public DistinctCountULLAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    int numExpressions = arguments.size();
    // This function expects 1 or 2 arguments.
    Preconditions.checkArgument(numExpressions <= 2, "DistinctCountULL expects 1 or 2 arguments, got: %s",
        numExpressions);
    if (arguments.size() == 2) {
      _p = arguments.get(1).getLiteral().getIntValue();
    } else {
      _p = CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P;
    }
  }

  public int getP() {
    return _p;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTULL;
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
      // Logical BYTES is a serialized UltraLogLog and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          int i = from;
          UltraLogLog ull = aggregationResultHolder.getResult();
          if (ull == null) {
            if (i == to) {
              return;
            }
            // The first UltraLogLog read becomes the accumulator instead of being merged into a fresh one
            ull = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytesValues[i++]);
            aggregationResultHolder.setValue(ull);
          }
          for (; i < to; i++) {
            ull.add(ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytesValues[i]));
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging UltraLogLogs", e);
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

    // For non-dictionary-encoded expression, store values into the UltraLogLog
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(intValues[i]).ifPresent(ull::add);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(longValues[i]).ifPresent(ull::add);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(floatValues[i]).ifPresent(ull::add);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(doubleValues[i]).ifPresent(ull::add);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(stringValues[i]).ifPresent(ull::add);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          UltraLogLog ull = getULL(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(bytesValues[i]).ifPresent(ull::add);
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
    }
  }

  protected void aggregateMV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      DataType storedType) {
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          dictIdBitmap.add(dictIds[i]);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the UltraLogLog
    UltraLogLog ull = getULL(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int value : intValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (long value : longValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (float value : floatValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (double value : doubleValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (String value : stringValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (byte[] value : bytesValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized UltraLogLog and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          for (int i = from; i < to; i++) {
            UltraLogLog value = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytesValues[i]);
            int groupKey = groupKeyArray[i];
            UltraLogLog ull = groupByResultHolder.getResult(groupKey);
            if (ull != null) {
              ull.add(value);
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging UltraLogLog", e);
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

    // For non-dictionary-encoded expression, store values into the UltraLogLog
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(intValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(longValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(floatValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(doubleValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(stringValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLogUtils.hashObject(bytesValues[i])
                .ifPresent(getULL(groupByResultHolder, groupKeyArray[i])::add);
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store values into the UltraLogLog
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (int value : intValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (long value : longValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (float value : floatValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (double value : doubleValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (String value : stringValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            UltraLogLog ull = getULL(groupByResultHolder, groupKeyArray[i]);
            for (byte[] value : bytesValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized UltraLogLog and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        try {
          for (int i = from; i < to; i++) {
            UltraLogLog value = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytesValues[i]);
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = groupByResultHolder.getResult(groupKey);
              if (ull != null) {
                ull.add(value);
              } else {
                groupByResultHolder.setValueForKey(groupKey,
                    ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytesValues[i]));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging UltraLogLog", e);
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

    // For non-dictionary-encoded expression, store values into the UltraLogLog
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
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store values into the UltraLogLog
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            int[] intRow = intValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (int value : intRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            long[] longRow = longValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (long value : longRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            float[] floatRow = floatValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (float value : floatRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            double[] doubleRow = doubleValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (double value : doubleRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            String[] stringRow = stringValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (String value : stringRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            byte[][] bytesRow = bytesValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UltraLogLog ull = getULL(groupByResultHolder, groupKey);
              for (byte[] value : bytesRow) {
                UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
              }
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_ULL aggregation function: " + storedType);
    }
  }

  @Nullable
  @Override
  public UltraLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return null;
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to ULL
      return convertToULL((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the ULL
      return (UltraLogLog) result;
    }
  }

  @Nullable
  @Override
  public UltraLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return null;
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to HyperLogLogPlus
      return convertToULL((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the HyperLogLogPlus
      return (UltraLogLog) result;
    }
  }

  @Override
  public UltraLogLog merge(UltraLogLog intermediateResult1, UltraLogLog intermediateResult2) {
    // UltraLogLog object doesn't support merging a smaller p object into a larger p object.
    if (intermediateResult1.getP() > intermediateResult2.getP()) {
      return intermediateResult2.add(intermediateResult1);
    }
    return intermediateResult1.add(intermediateResult2);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(UltraLogLog ultraLogLog) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.UltraLogLog.getValue(),
        ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(ultraLogLog));
  }

  @Override
  public UltraLogLog deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Comparable extractFinalResult(@Nullable UltraLogLog intermediateResult) {
    return intermediateResult == null ? 0L : Math.round(intermediateResult.getDistinctCountEstimate());
  }

  @Override
  public Comparable mergeFinalResult(Comparable finalResult1, Comparable finalResult2) {
    return (Long) finalResult1 + (Long) finalResult2;
  }

  @Override
  public boolean canUseStarTree(Map<String, Object> functionParameters) {
    Object p = functionParameters.get(Constants.HLLPLUS_ULL_P_KEY);
    // Check if p value matches
    if (p != null) {
      return _p == Integer.parseInt(String.valueOf(p));
    } else {
      // If the functionParameters don't have an explicit p set, it means that the star-tree index was built with
      // the default value for p
      return _p == CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P;
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
  protected UltraLogLog getULL(AggregationResultHolder aggregationResultHolder) {
    UltraLogLog ull = aggregationResultHolder.getResult();
    if (ull == null) {
      ull = UltraLogLog.create(_p);
      aggregationResultHolder.setValue(ull);
    }
    return ull;
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
  protected UltraLogLog getULL(GroupByResultHolder groupByResultHolder, int groupKey) {
    UltraLogLog ull = groupByResultHolder.getResult(groupKey);
    if (ull == null) {
      ull = UltraLogLog.create(_p);
      groupByResultHolder.setValueForKey(groupKey, ull);
    }
    return ull;
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
      UltraLogLogUtils.hashObject(value)
          .ifPresent(getULL(groupByResultHolder, groupKey)::add);
    }
  }

  /// Helper method to read dictionary and convert dictionary ids to HyperLogLogPlus for dictionary-encoded expression.
  private UltraLogLog convertToULL(DictIdsWrapper dictIdsWrapper) {
    UltraLogLog ull = UltraLogLog.create(_p);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      UltraLogLogUtils.hashObject(dictionary.get(iterator.next()))
          .ifPresent(ull::add);
    }
    return ull;
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
