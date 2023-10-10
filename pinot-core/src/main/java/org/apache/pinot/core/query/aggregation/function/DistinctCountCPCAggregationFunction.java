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
import java.util.List;
import java.util.Map;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountCPCAggregationFunction extends BaseSingleInputAggregationFunction<CpcSketch, Comparable> {
  protected final int _lgK;

  public DistinctCountCPCAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 2 arguments.
    Preconditions.checkArgument(numExpressions <= 2, "DistinctCountCPC expects 1 or 2 arguments, got: %s",
        numExpressions);
    if (arguments.size() == 2) {
      _lgK = arguments.get(1).getLiteral().getIntValue();
    } else {
      _lgK = CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
    }
  }

  public int getLgK() {
    return _lgK;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTCPCSKETCH;
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

    // Treat BYTES value as serialized CPC Sketch
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        CpcSketch cpcSketch = aggregationResultHolder.getResult();
        CpcUnion union = new CpcUnion(_lgK);
        if (cpcSketch != null) {
          union.update(cpcSketch);
        }
        for (int i = 0; i < length; i++) {
          union.update(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytesValues[i]));
        }
        aggregationResultHolder.setValue(union.getResult());
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging CPC sketches", e);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the CpcSketch
    CpcSketch cpcSketch = getCpcSketch(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          cpcSketch.update(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          cpcSketch.update(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          cpcSketch.update(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          cpcSketch.update(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          cpcSketch.update(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_CPC aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized CPC Sketch
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          CpcSketch value = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytesValues[i]);
          int groupKey = groupKeyArray[i];
          CpcSketch cpcSketch = groupByResultHolder.getResult(groupKey);
          if (cpcSketch != null) {
            CpcUnion union = new CpcUnion(_lgK);
            union.update(cpcSketch);
            union.update(value);
            groupByResultHolder.setValueForKey(groupKey, union.getResult());
          } else {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging CPC sketches", e);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the CpcSketch
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getCpcSketch(groupByResultHolder, groupKeyArray[i]).update(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getCpcSketch(groupByResultHolder, groupKeyArray[i]).update(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getCpcSketch(groupByResultHolder, groupKeyArray[i]).update(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getCpcSketch(groupByResultHolder, groupKeyArray[i]).update(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getCpcSketch(groupByResultHolder, groupKeyArray[i]).update(stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_CPC aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized CPC Sketch
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        for (int i = 0; i < length; i++) {
          CpcSketch value = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            CpcSketch cpcSketch = groupByResultHolder.getResult(groupKey);
            if (cpcSketch != null) {
              CpcUnion union = new CpcUnion(_lgK);
              union.update(cpcSketch);
              union.update(value);
              groupByResultHolder.setValueForKey(groupKey, union.getResult());
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging CPC sketches", e);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the CpcSketch
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getCpcSketch(groupByResultHolder, groupKey).update(intValues[i]);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getCpcSketch(groupByResultHolder, groupKey).update(longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getCpcSketch(groupByResultHolder, groupKey).update(floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getCpcSketch(groupByResultHolder, groupKey).update(doubleValues[i]);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getCpcSketch(groupByResultHolder, groupKey).update(stringValues[i]);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_CPC aggregation function: " + storedType);
    }
  }

  @Override
  public CpcSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new CpcSketch(_lgK);
    } else {
      // For non-dictionary-encoded expression, directly return the CpcSketch
      return (CpcSketch) result;
    }
  }

  @Override
  public CpcSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new CpcSketch(_lgK);
    } else {
      // For non-dictionary-encoded expression, directly return the CpcSketch
      return (CpcSketch) result;
    }
  }

  @Override
  public CpcSketch merge(CpcSketch intermediateResult1, CpcSketch intermediateResult2) {
    if (intermediateResult1 == null && intermediateResult2 != null) {
      return intermediateResult2;
    } else if (intermediateResult1 != null && intermediateResult2 == null) {
      return intermediateResult1;
    } else if (intermediateResult1 == null) {
      return new CpcSketch(_lgK);
    }

    CpcUnion union = new CpcUnion(_lgK);
    union.update(intermediateResult1);
    union.update(intermediateResult2);
    return union.getResult();
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Comparable extractFinalResult(CpcSketch intermediateResult) {
    return Math.round(intermediateResult.getEstimate());
  }

  /**
   * Returns the CpcSketch from the result holder or creates a new one if it does not exist.
   */
  protected CpcSketch getCpcSketch(AggregationResultHolder aggregationResultHolder) {
    CpcSketch cpcSketch = aggregationResultHolder.getResult();
    if (cpcSketch == null) {
      cpcSketch = new CpcSketch(_lgK);
      aggregationResultHolder.setValue(cpcSketch);
    }
    return cpcSketch;
  }

  /**
   * Returns the CpcSketch for the given group key or creates a new one if it does not exist.
   */
  protected CpcSketch getCpcSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    CpcSketch cpcSketch = groupByResultHolder.getResult(groupKey);
    if (cpcSketch == null) {
      cpcSketch = new CpcSketch(_lgK);
      groupByResultHolder.setValueForKey(groupKey, cpcSketch);
    }
    return cpcSketch;
  }
}
