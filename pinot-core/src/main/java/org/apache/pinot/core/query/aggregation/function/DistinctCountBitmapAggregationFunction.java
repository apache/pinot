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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// The `DistinctCountBitmapAggregationFunction` calculates the number of distinct values for a given single-value or
/// multi-value expression using RoaringBitmap. The bitmap stores the actual values for `INT` expression, or hash code
/// of the values for other data types (values with the same hash code will only be counted once).
public class DistinctCountBitmapAggregationFunction extends BaseSingleInputAggregationFunction<RoaringBitmap, Integer> {

  public DistinctCountBitmapAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "DISTINCT_COUNT_BITMAP"), nullHandlingEnabled);
  }

  protected DistinctCountBitmapAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP;
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
      // Logical BYTES is a serialized RoaringBitmap and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        int i = from;
        RoaringBitmap valueBitmap = aggregationResultHolder.getResult();
        if (valueBitmap == null) {
          if (i == to) {
            return;
          }
          // The first bitmap read becomes the accumulator instead of being merged into a fresh one
          valueBitmap = RoaringBitmapUtils.deserialize(bytesValues[i++]);
          aggregationResultHolder.setValue(valueBitmap);
        }
        for (; i < to; i++) {
          valueBitmap.or(RoaringBitmapUtils.deserialize(bytesValues[i]));
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet,
            (from, to) -> getValueBitmap(aggregationResultHolder).addN(intValues, from, to - from));
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(Long.hashCode(longValues[i]));
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(Float.hashCode(floatValues[i]));
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(Double.hashCode(doubleValues[i]));
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(stringValues[i].hashCode());
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(Arrays.hashCode(bytesValues[i]));
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            valueBitmap.add(intValues[i]);
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (long value : longValues[i]) {
              valueBitmap.add(Long.hashCode(value));
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (float value : floatValues[i]) {
              valueBitmap.add(Float.hashCode(value));
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (double value : doubleValues[i]) {
              valueBitmap.add(Double.hashCode(value));
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (String value : stringValues[i]) {
              valueBitmap.add(value.hashCode());
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
          for (int i = from; i < to; i++) {
            for (byte[] value : bytesValues[i]) {
              valueBitmap.add(Arrays.hashCode(value));
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized RoaringBitmap and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          RoaringBitmap value = RoaringBitmapUtils.deserialize(bytesValues[i]);
          int groupKey = groupKeyArray[i];
          RoaringBitmap valueBitmap = groupByResultHolder.getResult(groupKey);
          if (valueBitmap != null) {
            valueBitmap.or(value);
          } else {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Long.hashCode(longValues[i]));
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Float.hashCode(floatValues[i]));
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Double.hashCode(doubleValues[i]));
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(stringValues[i].hashCode());
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Arrays.hashCode(bytesValues[i]));
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(intValues[i]);
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
            for (long value : longValues[i]) {
              bitmap.add(Long.hashCode(value));
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
            for (float value : floatValues[i]) {
              bitmap.add(Float.hashCode(value));
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
            for (double value : doubleValues[i]) {
              bitmap.add(Double.hashCode(value));
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
            for (String value : stringValues[i]) {
              bitmap.add(value.hashCode());
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
            for (byte[] value : bytesValues[i]) {
              bitmap.add(Arrays.hashCode(value));
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Logical BYTES is a serialized RoaringBitmap and always uses the single-value representation.
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          RoaringBitmap value = RoaringBitmapUtils.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            RoaringBitmap bitmap = groupByResultHolder.getResult(groupKey);
            if (bitmap != null) {
              bitmap.or(value);
            } else {
              // Clone a bitmap for the group
              groupByResultHolder.setValueForKey(groupKey, value.clone());
            }
          }
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
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
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Long.hashCode(longValues[i]));
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Float.hashCode(floatValues[i]));
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Double.hashCode(doubleValues[i]));
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i].hashCode());
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Arrays.hashCode(bytesValues[i]));
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              getValueBitmap(groupByResultHolder, groupKey).add(intValues[i]);
            }
          }
        });
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
              for (long value : longValues[i]) {
                bitmap.add(Long.hashCode(value));
              }
            }
          }
        });
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
              for (float value : floatValues[i]) {
                bitmap.add(Float.hashCode(value));
              }
            }
          }
        });
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
              for (double value : doubleValues[i]) {
                bitmap.add(Double.hashCode(value));
              }
            }
          }
        });
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
              for (String value : stringValues[i]) {
                bitmap.add(value.hashCode());
              }
            }
          }
        });
        break;
      case BYTES:
        byte[][][] bytesValues = blockValSet.getBytesValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              RoaringBitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
              for (byte[] value : bytesValues[i]) {
                bitmap.add(Arrays.hashCode(value));
              }
            }
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public RoaringBitmap extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new RoaringBitmap();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (RoaringBitmap) result;
    }
  }

  @Override
  public RoaringBitmap extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new RoaringBitmap();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (RoaringBitmap) result;
    }
  }

  @Override
  public RoaringBitmap merge(RoaringBitmap intermediateResult1, RoaringBitmap intermediateResult2) {
    intermediateResult1.or(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(RoaringBitmap roaringBitmap) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.RoaringBitmap.getValue(),
        ObjectSerDeUtils.ROARING_BITMAP_SER_DE.serialize(roaringBitmap));
  }

  @Override
  public RoaringBitmap deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.ROARING_BITMAP_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(@Nullable RoaringBitmap intermediateResult) {
    return intermediateResult == null ? 0 : intermediateResult.getCardinality();
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
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

  /// Returns the value bitmap from the result holder or creates a new one if it does not exist.
  protected static RoaringBitmap getValueBitmap(AggregationResultHolder aggregationResultHolder) {
    RoaringBitmap bitmap = aggregationResultHolder.getResult();
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      aggregationResultHolder.setValue(bitmap);
    }
    return bitmap;
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

  /// Returns the value bitmap for the given group key or creates a new one if it does not exist.
  protected static RoaringBitmap getValueBitmap(GroupByResultHolder groupByResultHolder, int groupKey) {
    RoaringBitmap bitmap = groupByResultHolder.getResult(groupKey);
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      groupByResultHolder.setValueForKey(groupKey, bitmap);
    }
    return bitmap;
  }

  /// Helper method to set dictionary id for the given group keys into the result holder.
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /// Helper method to set value for the given group keys into the result holder.
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      getValueBitmap(groupByResultHolder, groupKey).add(value);
    }
  }

  /// Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
  /// expression.
  private static RoaringBitmap convertToValueBitmap(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    RoaringBitmap valueBitmap = new RoaringBitmap();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getIntValue(iterator.next()));
        }
        break;
      case LONG:
        while (iterator.hasNext()) {
          valueBitmap.add(Long.hashCode(dictionary.getLongValue(iterator.next())));
        }
        break;
      case FLOAT:
        while (iterator.hasNext()) {
          valueBitmap.add(Float.hashCode(dictionary.getFloatValue(iterator.next())));
        }
        break;
      case DOUBLE:
        while (iterator.hasNext()) {
          valueBitmap.add(Double.hashCode(dictionary.getDoubleValue(iterator.next())));
        }
        break;
      case STRING:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getStringValue(iterator.next()).hashCode());
        }
        break;
      case BYTES:
        while (iterator.hasNext()) {
          valueBitmap.add(Arrays.hashCode(dictionary.getBytesValue(iterator.next())));
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
    return valueBitmap;
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
