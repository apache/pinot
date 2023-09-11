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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Collection;
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
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code SegmentPartitionedDistinctCountAggregationFunction} calculates the number of distinct values for a given
 * single-value expression.
 * <p>IMPORTANT: This function relies on the expression values being partitioned for each segment, where there is no
 * common values within different segments.
 * <p>This function calculates the exact number of distinct values within the segment, then simply sums up the results
 * from different segments to get the final result.
 */
public class SegmentPartitionedDistinctCountAggregationFunction extends BaseSingleInputAggregationFunction<Long, Long> {

  public SegmentPartitionedDistinctCountAggregationFunction(List<ExpressionContext> arguments) {
    super(verifyArguments(arguments));
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "SEGMENT_PARTITIONED_DISTINCT_COUNT expects 1 argument, got: %s",
        arguments.size());
    return arguments.get(0);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SEGMENTPARTITIONEDDISTINCTCOUNT;
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

    // For dictionary-encoded expression, store dictionary ids into a RoaringBitmap
    if (blockValSet.getDictionary() != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      RoaringBitmap bitmap = aggregationResultHolder.getResult();
      if (bitmap == null) {
        bitmap = new RoaringBitmap();
        aggregationResultHolder.setValue(bitmap);
      }
      bitmap.addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression, store INT values into a RoaringBitmap, other types into an OpenHashSet
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        RoaringBitmap bitmap = aggregationResultHolder.getResult();
        if (bitmap == null) {
          bitmap = new RoaringBitmap();
          aggregationResultHolder.setValue(bitmap);
        }
        bitmap.addN(intValues, 0, length);
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        LongOpenHashSet longSet = aggregationResultHolder.getResult();
        if (longSet == null) {
          longSet = new LongOpenHashSet();
          aggregationResultHolder.setValue(longSet);
        }
        for (int i = 0; i < length; i++) {
          longSet.add(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        FloatOpenHashSet floatSet = aggregationResultHolder.getResult();
        if (floatSet == null) {
          floatSet = new FloatOpenHashSet();
          aggregationResultHolder.setValue(floatSet);
        }
        for (int i = 0; i < length; i++) {
          floatSet.add(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        DoubleOpenHashSet doubleSet = aggregationResultHolder.getResult();
        if (doubleSet == null) {
          doubleSet = new DoubleOpenHashSet();
          aggregationResultHolder.setValue(doubleSet);
        }
        for (int i = 0; i < length; i++) {
          doubleSet.add(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        ObjectOpenHashSet<String> stringSet = aggregationResultHolder.getResult();
        if (stringSet == null) {
          stringSet = new ObjectOpenHashSet<>();
          aggregationResultHolder.setValue(stringSet);
        }
        //noinspection ManualArrayToCollectionCopy
        for (int i = 0; i < length; i++) {
          stringSet.add(stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        ObjectOpenHashSet<ByteArray> bytesSet = aggregationResultHolder.getResult();
        if (bytesSet == null) {
          bytesSet = new ObjectOpenHashSet<>();
          aggregationResultHolder.setValue(bytesSet);
        }
        for (int i = 0; i < length; i++) {
          bytesSet.add(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for PARTITIONED_DISTINCT_COUNT aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into a RoaringBitmap
    if (blockValSet.getDictionary() != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setIntValueForGroup(groupByResultHolder, groupKeyArray[i], dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store INT values into a RoaringBitmap, other types into an OpenHashSet
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setIntValueForGroup(groupByResultHolder, groupKeyArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setLongValueForGroup(groupByResultHolder, groupKeyArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setFloatValueForGroup(groupByResultHolder, groupKeyArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setDoubleValueForGroup(groupByResultHolder, groupKeyArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setStringValueForGroup(groupByResultHolder, groupKeyArray[i], stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          setBytesValueForGroup(groupByResultHolder, groupKeyArray[i], new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for PARTITIONED_DISTINCT_COUNT aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into a RoaringBitmap
    if (blockValSet.getDictionary() != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        int dictId = dictIds[i];
        for (int groupKey : groupKeysArray[i]) {
          setIntValueForGroup(groupByResultHolder, groupKey, dictId);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store INT values into a RoaringBitmap, other types into an OpenHashSet
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          int value = intValues[i];
          for (int groupKey : groupKeysArray[i]) {
            setIntValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            setLongValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          float value = floatValues[i];
          for (int groupKey : groupKeysArray[i]) {
            setFloatValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            setDoubleValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            setStringValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          ByteArray value = new ByteArray(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            setBytesValueForGroup(groupByResultHolder, groupKey, value);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for PARTITIONED_DISTINCT_COUNT aggregation function: " + storedType);
    }
  }

  @Override
  public Long extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return extractIntermediateResult(aggregationResultHolder.getResult());
  }

  @Override
  public Long extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
  }

  @Override
  public Long merge(Long intermediateResult1, Long intermediateResult2) {
    return intermediateResult1 + intermediateResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(Long intermediateResult) {
    return intermediateResult;
  }

  /**
   * Helper method to set an INT value for the given group key into the result holder.
   */
  private static void setIntValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    RoaringBitmap bitmap = groupByResultHolder.getResult(groupKey);
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      groupByResultHolder.setValueForKey(groupKey, bitmap);
    }
    bitmap.add(value);
  }

  /**
   * Helper method to set an LONG value for the given group key into the result holder.
   */
  private static void setLongValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, long value) {
    LongOpenHashSet longSet = groupByResultHolder.getResult(groupKey);
    if (longSet == null) {
      longSet = new LongOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, longSet);
    }
    longSet.add(value);
  }

  /**
   * Helper method to set an FLOAT value for the given group key into the result holder.
   */
  private static void setFloatValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, float value) {
    FloatOpenHashSet floatSet = groupByResultHolder.getResult(groupKey);
    if (floatSet == null) {
      floatSet = new FloatOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, floatSet);
    }
    floatSet.add(value);
  }

  /**
   * Helper method to set an DOUBLE value for the given group key into the result holder.
   */
  private static void setDoubleValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, double value) {
    DoubleOpenHashSet doubleSet = groupByResultHolder.getResult(groupKey);
    if (doubleSet == null) {
      doubleSet = new DoubleOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, doubleSet);
    }
    doubleSet.add(value);
  }

  /**
   * Helper method to set an STRING value for the given group key into the result holder.
   */
  private static void setStringValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, String value) {
    ObjectOpenHashSet<String> stringSet = groupByResultHolder.getResult(groupKey);
    if (stringSet == null) {
      stringSet = new ObjectOpenHashSet<>();
      groupByResultHolder.setValueForKey(groupKey, stringSet);
    }
    stringSet.add(value);
  }

  /**
   * Helper method to set an BYTES value for the given group key into the result holder.
   */
  private static void setBytesValueForGroup(GroupByResultHolder groupByResultHolder, int groupKey, ByteArray value) {
    ObjectOpenHashSet<ByteArray> bytesSet = groupByResultHolder.getResult(groupKey);
    if (bytesSet == null) {
      bytesSet = new ObjectOpenHashSet<>();
      groupByResultHolder.setValueForKey(groupKey, bytesSet);
    }
    bytesSet.add(value);
  }

  /**
   * Helper method to extract segment level intermediate result from the inner segment result.
   */
  private static long extractIntermediateResult(@Nullable Object result) {
    if (result == null) {
      return 0L;
    }
    if (result instanceof RoaringBitmap) {
      return ((RoaringBitmap) result).getLongCardinality();
    }
    assert result instanceof Collection;
    return ((Collection<?>) result).size();
  }
}
