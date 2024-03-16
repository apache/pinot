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
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * <p>
 *  {@code PercentileKLLAggregationFunction} provides an approximate percentile calculator using the KLL algorithm
 *  from <a href="https://datasketches.apache.org/docs/KLL/KLLSketch.html">Apache DataSketches library</a>.
 * </p>
 * <p>
 *  The interface is similar to plain 'Percentile' function except for the optional K value which determines
 *  the size, hence the accuracy of the sketch.
 * </p>
 * <p><b>PERCENTILE_KLL(col, percentile, kValue)</b></p>
 * <p>E.g.:</p>
 * <ul>
 *   <li><b>PERCENTILE_KLL(col, 90)</b></li>
 *   <li><b>PERCENTILE_KLL(col, 99.9, 800)</b></li>
 * </ul>
 *
 * <p>
 *   If the column type is BYTES, the aggregation function will assume it is a serialized KllDoubleSketch and will
 *   attempt to deserialize it for further processing.
 * </p>
 *
 * <p>
 *   There is a variation of the function (<b>PERCENTILE_RAW_KLL</b>) that returns the Base64 encoded
 *   sketch object to be used externally.
 * </p>
 */
public class PercentileKLLAggregationFunction
    extends NullableSingleInputAggregationFunction<KllDoublesSketch, Comparable<?>> {
  protected static final int DEFAULT_K_VALUE = 200;

  protected final double _percentile;
  protected int _kValue;

  public PercentileKLLAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);

    // Check that there are correct number of arguments
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 2 || numArguments == 3,
        "Expecting 2 or 3 arguments for PercentileKLL function: PERCENTILE_KLL(column, percentile, k=200");

    _percentile = arguments.get(1).getLiteral().getDoubleValue();
    Preconditions.checkArgument(_percentile >= 0 && _percentile <= 100,
        "Percentile value needs to be in range 0-100, inclusive");

    _kValue = numArguments == 3 ? arguments.get(2).getLiteral().getIntValue() : DEFAULT_K_VALUE;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEKLL;
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
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();
    KllDoublesSketch sketch = getOrCreateSketch(aggregationResultHolder);

    if (valueType == DataType.BYTES) {
      // Assuming the column contains serialized data sketch
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          sketch.merge(deserializedSketches[i]);
        }
      });
    } else {
      double[] values = valueSet.getDoubleValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          sketch.update(values[i]);
        }
      });
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();

    if (valueType == DataType.BYTES) {
      // serialized sketch
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
          sketch.merge(deserializedSketches[i]);
        }
      });
    } else {
      double[] values = valueSet.getDoubleValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
          sketch.update(values[i]);
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();

    if (valueType == DataType.BYTES) {
      // serialized sketch
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
            sketch.merge(deserializedSketches[i]);
          }
        }
      });
    } else {
      double[] values = valueSet.getDoubleValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
            sketch.update(values[i]);
          }
        }
      });
    }
  }

  /**
   * Extracts the sketch from the result holder or creates a new one if it does not exist.
   */
  protected KllDoublesSketch getOrCreateSketch(AggregationResultHolder aggregationResultHolder) {
    KllDoublesSketch sketch = aggregationResultHolder.getResult();
    if (sketch == null) {
      sketch = KllDoublesSketch.newHeapInstance(_kValue);
      aggregationResultHolder.setValue(sketch);
    }
    return sketch;
  }

  /**
   * Extracts the sketch from the group by result holder for key
   * or creates a new one if it does not exist.
   */
  protected KllDoublesSketch getOrCreateSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    KllDoublesSketch sketch = groupByResultHolder.getResult(groupKey);
    if (sketch == null) {
      sketch = KllDoublesSketch.newHeapInstance(_kValue);
      groupByResultHolder.setValueForKey(groupKey, sketch);
    }
    return sketch;
  }

  /**
   * Deserializes the sketches from the bytes.
   */
  protected KllDoublesSketch[] deserializeSketches(byte[][] serializedSketches) {
    KllDoublesSketch[] sketches = new KllDoublesSketch[serializedSketches.length];
    for (int i = 0; i < serializedSketches.length; i++) {
      sketches[i] = KllDoublesSketch.wrap(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  @Override
  public KllDoublesSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public KllDoublesSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public KllDoublesSketch merge(KllDoublesSketch sketch1, KllDoublesSketch sketch2) {
    KllDoublesSketch union = KllDoublesSketch.newHeapInstance(_kValue);
    if (sketch1 != null) {
      union.merge(sketch1);
    }
    if (sketch2 != null) {
      union.merge(sketch2);
    }
    return union;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.PERCENTILEKLL.getName().toLowerCase() + "(" + _expression + ", " + _percentile + ")";
  }

  @Override
  public Comparable<?> extractFinalResult(KllDoublesSketch sketch) {
    if (sketch.isEmpty() && _nullHandlingEnabled) {
      return null;
    }
    return sketch.getQuantile(_percentile / 100);
  }
}
