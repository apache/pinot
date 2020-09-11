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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.ThetaSketchAggregationFunction.Parameters;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The {@code RawThetaSketchAggregationFunction} collects the values for a given expression (can be single-valued or
 * multi-valued) into a {@link Sketch} object, and returns the sketch as a base64 encoded string. It treats BYTES
 * expression as serialized sketches.
 * <p>The function takes an optional second argument as the parameters for the function. Currently there is only 1
 * parameter for the function:
 * <ul>
 *   <li>
 *     nominalEntries: The nominal entries used to create the sketch. (Default 4096)
 *   </li>
 * </ul>
 * <p>Example: RAW_THETA_SKETCH(col, 'nominalEntries=8192')
 */
public class RawThetaSketchAggregationFunction extends BaseSingleInputAggregationFunction<Sketch, String> {
  private final UpdateSketchBuilder _updateSketchBuilder = new UpdateSketchBuilder();
  private final SetOperationBuilder _setOperationBuilder = new SetOperationBuilder();

  public RawThetaSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));

    // Optional second argument for theta-sketch parameters
    if (arguments.size() > 1) {
      ExpressionContext paramsExpression = arguments.get(1);
      Preconditions.checkArgument(paramsExpression.getType() == ExpressionContext.Type.LITERAL,
          "Second argument of RAW_THETA_SKETCH aggregation function must be literal (parameters)");
      Parameters parameters = new Parameters(paramsExpression.getLiteral());
      int nominalEntries = parameters.getNominalEntries();
      _updateSketchBuilder.setNominalEntries(nominalEntries);
      _setOperationBuilder.setNominalEntries(nominalEntries);
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.RAWTHETASKETCH;
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
    DataType valueType = blockValSet.getValueType();

    if (valueType != DataType.BYTES) {
      UpdateSketch updateSketch = getUpdateSketch(aggregationResultHolder);
      if (blockValSet.isSingleValue()) {
        switch (valueType) {
          case INT:
            int[] intValues = blockValSet.getIntValuesSV();
            for (int i = 0; i < length; i++) {
              updateSketch.update(intValues[i]);
            }
            break;
          case LONG:
            long[] longValues = blockValSet.getLongValuesSV();
            for (int i = 0; i < length; i++) {
              updateSketch.update(longValues[i]);
            }
            break;
          case FLOAT:
            float[] floatValues = blockValSet.getFloatValuesSV();
            for (int i = 0; i < length; i++) {
              updateSketch.update(floatValues[i]);
            }
            break;
          case DOUBLE:
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            for (int i = 0; i < length; i++) {
              updateSketch.update(doubleValues[i]);
            }
            break;
          case STRING:
            String[] stringValues = blockValSet.getStringValuesSV();
            for (int i = 0; i < length; i++) {
              updateSketch.update(stringValues[i]);
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal single-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
        }
      } else {
        switch (valueType) {
          case INT:
            int[][] intValues = blockValSet.getIntValuesMV();
            for (int i = 0; i < length; i++) {
              for (int value : intValues[i]) {
                updateSketch.update(value);
              }
            }
            break;
          case LONG:
            long[][] longValues = blockValSet.getLongValuesMV();
            for (int i = 0; i < length; i++) {
              for (long value : longValues[i]) {
                updateSketch.update(value);
              }
            }
            break;
          case FLOAT:
            float[][] floatValues = blockValSet.getFloatValuesMV();
            for (int i = 0; i < length; i++) {
              for (float value : floatValues[i]) {
                updateSketch.update(value);
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValues = blockValSet.getDoubleValuesMV();
            for (int i = 0; i < length; i++) {
              for (double value : doubleValues[i]) {
                updateSketch.update(value);
              }
            }
            break;
          case STRING:
            String[][] stringValues = blockValSet.getStringValuesMV();
            for (int i = 0; i < length; i++) {
              for (String value : stringValues[i]) {
                updateSketch.update(value);
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal multi-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
        }
      }
    } else {
      // Serialized sketch
      Union union = getUnion(aggregationResultHolder);
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        union.update(Sketch.wrap(Memory.wrap(bytesValues[i])));
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType valueType = blockValSet.getValueType();

    if (blockValSet.isSingleValue()) {
      switch (valueType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            getUpdateSketch(groupByResultHolder, groupKeyArray[i]).update(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            getUpdateSketch(groupByResultHolder, groupKeyArray[i]).update(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            getUpdateSketch(groupByResultHolder, groupKeyArray[i]).update(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            getUpdateSketch(groupByResultHolder, groupKeyArray[i]).update(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            getUpdateSketch(groupByResultHolder, groupKeyArray[i]).update(stringValues[i]);
          }
          break;
        case BYTES:
          // Serialized sketch
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            getUnion(groupByResultHolder, groupKeyArray[i]).update(Sketch.wrap(Memory.wrap(bytesValues[i])));
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal single-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
      }
    } else {
      switch (valueType) {
        case INT:
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKeyArray[i]);
            for (int value : intValues[i]) {
              updateSketch.update(value);
            }
          }
          break;
        case LONG:
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKeyArray[i]);
            for (long value : longValues[i]) {
              updateSketch.update(value);
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKeyArray[i]);
            for (float value : floatValues[i]) {
              updateSketch.update(value);
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKeyArray[i]);
            for (double value : doubleValues[i]) {
              updateSketch.update(value);
            }
          }
          break;
        case STRING:
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKeyArray[i]);
            for (String value : stringValues[i]) {
              updateSketch.update(value);
            }
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal multi-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType valueType = blockValSet.getValueType();

    if (blockValSet.isSingleValue()) {
      switch (valueType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            int value = intValues[i];
            for (int groupKey : groupKeysArray[i]) {
              getUpdateSketch(groupByResultHolder, groupKey).update(value);
            }
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            long value = longValues[i];
            for (int groupKey : groupKeysArray[i]) {
              getUpdateSketch(groupByResultHolder, groupKey).update(value);
            }
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            float value = floatValues[i];
            for (int groupKey : groupKeysArray[i]) {
              getUpdateSketch(groupByResultHolder, groupKey).update(value);
            }
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            double value = doubleValues[i];
            for (int groupKey : groupKeysArray[i]) {
              getUpdateSketch(groupByResultHolder, groupKey).update(value);
            }
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            String value = stringValues[i];
            for (int groupKey : groupKeysArray[i]) {
              getUpdateSketch(groupByResultHolder, groupKey).update(value);
            }
          }
          break;
        case BYTES:
          // Serialized sketch
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            Sketch sketch = Sketch.wrap(Memory.wrap(bytesValues[i]));
            for (int groupKey : groupKeysArray[i]) {
              getUnion(groupByResultHolder, groupKey).update(sketch);
            }
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal single-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
      }
    } else {
      switch (valueType) {
        case INT:
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            int[] values = intValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKey);
              for (int value : values) {
                updateSketch.update(value);
              }
            }
          }
          break;
        case LONG:
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            long[] values = longValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKey);
              for (long value : values) {
                updateSketch.update(value);
              }
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            float[] values = floatValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKey);
              for (float value : values) {
                updateSketch.update(value);
              }
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            double[] values = doubleValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKey);
              for (double value : values) {
                updateSketch.update(value);
              }
            }
          }
          break;
        case STRING:
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            String[] values = stringValues[i];
            for (int groupKey : groupKeysArray[i]) {
              UpdateSketch updateSketch = getUpdateSketch(groupByResultHolder, groupKey);
              for (String value : values) {
                updateSketch.update(value);
              }
            }
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal multi-value data type for RAW_THETA_SKETCH aggregation function: " + valueType);
      }
    }
  }

  @Override
  public Sketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return ThetaSketchAggregationFunction.EMPTY_SKETCH;
    } else {
      if (result instanceof Sketch) {
        return (Sketch) result;
      } else {
        assert result instanceof Union;
        // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
        //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
        return ((Union) result).getResult(false, null);
      }
    }
  }

  @Override
  public Sketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result instanceof Sketch) {
      return (Sketch) result;
    } else {
      assert result instanceof Union;
      // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
      //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
      return ((Union) result).getResult(false, null);
    }
  }

  @Override
  public Sketch merge(Sketch sketch1, Sketch sketch2) {
    if (sketch1.isEmpty()) {
      return sketch2;
    }
    if (sketch2.isEmpty()) {
      return sketch1;
    }
    Union union = _setOperationBuilder.buildUnion();
    union.update(sketch1);
    union.update(sketch2);
    // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
    //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
    return union.getResult(false, null);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(Sketch sketch) {
    // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
    //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
    return Base64.getEncoder().encodeToString(sketch.compact(false, null).toByteArray());
  }

  /**
   * Returns the UpdateSketch from the result holder or creates a new one if it does not exist.
   */
  private UpdateSketch getUpdateSketch(AggregationResultHolder aggregationResultHolder) {
    UpdateSketch updateSketch = aggregationResultHolder.getResult();
    if (updateSketch == null) {
      updateSketch = _updateSketchBuilder.build();
      aggregationResultHolder.setValue(updateSketch);
    }
    return updateSketch;
  }

  /**
   * Returns the Union from the result holder or creates a new one if it does not exist.
   */
  private Union getUnion(AggregationResultHolder aggregationResultHolder) {
    Union union = aggregationResultHolder.getResult();
    if (union == null) {
      union = _setOperationBuilder.buildUnion();
      aggregationResultHolder.setValue(union);
    }
    return union;
  }

  /**
   * Returns the UpdateSketch for the given group key or creates a new one if it does not exist.
   */
  private UpdateSketch getUpdateSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    UpdateSketch updateSketch = groupByResultHolder.getResult(groupKey);
    if (updateSketch == null) {
      updateSketch = _updateSketchBuilder.build();
      groupByResultHolder.setValueForKey(groupKey, updateSketch);
    }
    return updateSketch;
  }

  /**
   * Returns the Union for the given group key or creates a new one if it does not exist.
   */
  private Union getUnion(GroupByResultHolder groupByResultHolder, int groupKey) {
    Union union = groupByResultHolder.getResult(groupKey);
    if (union == null) {
      union = _setOperationBuilder.buildUnion();
      groupByResultHolder.setValueForKey(groupKey, union);
    }
    return union;
  }
}
