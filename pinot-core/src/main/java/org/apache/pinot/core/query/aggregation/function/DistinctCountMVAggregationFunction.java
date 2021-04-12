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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountMVAggregationFunction extends DistinctCountAggregationFunction {

  public DistinctCountMVAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        dictIdBitmap.add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    FieldSpec.DataType valueType = blockValSet.getValueType();
    Set valueSet = getValueSet(aggregationResultHolder, valueType);
    switch (valueType) {
      case INT:
        IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValues[i]) {
            intSet.add(value);
          }
        }
        break;
      case LONG:
        LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValues[i]) {
            longSet.add(value);
          }
        }
        break;
      case FLOAT:
        FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            floatSet.add(value);
          }
        }
      case DOUBLE:
        DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            doubleSet.add(value);
          }
        }
        break;
      case STRING:
        ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          //noinspection ManualArrayToCollectionCopy
          for (String value : stringValues[i]) {
            //noinspection UseBulkOperation
            stringSet.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    FieldSpec.DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet intSet =
              (IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], FieldSpec.DataType.INT);
          for (int value : intValues[i]) {
            intSet.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          LongOpenHashSet longSet =
              (LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], FieldSpec.DataType.LONG);
          for (long value : longValues[i]) {
            longSet.add(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          FloatOpenHashSet floatSet =
              (FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], FieldSpec.DataType.FLOAT);
          for (float value : floatValues[i]) {
            floatSet.add(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          DoubleOpenHashSet doubleSet =
              (DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], FieldSpec.DataType.DOUBLE);
          for (double value : doubleValues[i]) {
            doubleSet.add(value);
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          ObjectOpenHashSet<String> stringSet =
              (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], FieldSpec.DataType.STRING);
          //noinspection ManualArrayToCollectionCopy
          for (String value : stringValues[i]) {
            //noinspection UseBulkOperation
            stringSet.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    FieldSpec.DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet intSet = (IntOpenHashSet) getValueSet(groupByResultHolder, groupKey, FieldSpec.DataType.INT);
            for (int value : intValues[i]) {
              intSet.add(value);
            }
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            LongOpenHashSet longSet =
                (LongOpenHashSet) getValueSet(groupByResultHolder, groupKey, FieldSpec.DataType.LONG);
            for (long value : longValues[i]) {
              longSet.add(value);
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            FloatOpenHashSet floatSet =
                (FloatOpenHashSet) getValueSet(groupByResultHolder, groupKey, FieldSpec.DataType.FLOAT);
            for (float value : floatValues[i]) {
              floatSet.add(value);
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            DoubleOpenHashSet doubleSet =
                (DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKey, FieldSpec.DataType.DOUBLE);
            for (double value : doubleValues[i]) {
              doubleSet.add(value);
            }
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            ObjectOpenHashSet<String> stringSet =
                (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKey, FieldSpec.DataType.STRING);
            //noinspection ManualArrayToCollectionCopy
            for (String value : stringValues[i]) {
              //noinspection UseBulkOperation
              stringSet.add(value);
            }
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }
}
