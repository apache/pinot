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
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.SerializedFrequentLongsSketch;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * <p>
 *  {@code FrequentLongsSketchAggregationFunction} provides an approximate FrequentItems aggregation function based on
 *  <a href="https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html">Apache DataSketches library</a>.
 *  It is memory efficient compared to exact counting.
 * </p>
 * <p>
 *   The function takes an INT or LONG column as input and returns a Base64 encoded sketch object which can be
 *   deserialized and used to estimate the frequency of items in the dataset (how many times they appear).
 * </p>
 * <p><b>FREQUENT_STRINGS_SKETCH(col, maxMapSize=256)</b></p>
 * <p>E.g.:</p>
 * <ul>
 *   <li><b>FREQUENT_LONGS_SKETCH(col)</b></li>
 *   <li><b>FREQUENT_LONGS_SKETCH(col, 1024)</b></li>
 * </ul>
 *
 * <p>
 *   If the column type is BYTES, the aggregation function will assume it is a serialized FrequentItems data sketch
 *   of type `LongsSketch`and will attempt to deserialize it for merging with other sketch objects.
 * </p>
 *
 * <p>
 *   Second argument, maxMapsSize, refers to the size of the physical length of the hashmap which stores counts. It
 *   influences the accuracy of the sketch and should be a power of 2.
 * </p>
 *
 * <p>
 *   There is a variation of the function (<b>FREQUENT_STRINGS_SKETCH</b>) which accepts STRING type input columns.
 * </p>
 */
public class FrequentLongsSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<LongsSketch, Comparable<?>> {
  protected static final int DEFAULT_MAX_MAP_SIZE = 256;

  protected int _maxMapSize;

  public FrequentLongsSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 1 || numArguments == 2,
        "Expecting 1 or 2 arguments for FrequentLongsSketch function: FREQUENTITEMSSKETCH(column, maxMapSize");
    _maxMapSize = numArguments == 2 ? arguments.get(1).getLiteral().getIntValue() : DEFAULT_MAX_MAP_SIZE;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FREQUENTLONGSSKETCH;
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
    FieldSpec.DataType valueType = valueSet.getValueType();

    LongsSketch sketch = getOrCreateSketch(aggregationResultHolder);

    switch (valueType) {
      case BYTES:
        // Assuming the column contains serialized data sketch
        LongsSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
        sketch = getOrCreateSketch(aggregationResultHolder);

        for (LongsSketch colSketch : deserializedSketches) {
          sketch.merge(colSketch);
        }
        break;
      case INT:
      case LONG:
        for (Long val : valueSet.getLongValuesSV()) {
          sketch.update(val);
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot aggregate on non int/long types");
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    FieldSpec.DataType valueType = valueSet.getValueType();

    switch (valueType) {
      case BYTES:
        // serialized sketch
        LongsSketch[] deserializedSketches =
            deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
        for (int i = 0; i < length; i++) {
          LongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
          sketch.merge(deserializedSketches[i]);
        }
        break;
      case INT:
      case LONG:
        long[] values = valueSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          LongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
          sketch.update(values[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot aggregate on non int/long types");
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    FieldSpec.DataType valueType = valueSet.getValueType();

    switch (valueType) {
      case BYTES:
        // serialized sketch
        LongsSketch[] deserializedSketches =
            deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            LongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
            sketch.merge(deserializedSketches[i]);
          }
        }
        break;
    case INT:
    case LONG:
      long[] values = valueSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          LongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
          sketch.update(values[i]);
        }
      }
      break;
    default:
      throw new UnsupportedOperationException("Cannot aggregate on non int/long types");
    }
  }

  /**
   * Extracts the sketch from the result holder or creates a new one if it does not exist.
   */
  protected LongsSketch getOrCreateSketch(AggregationResultHolder aggregationResultHolder) {
    LongsSketch sketch = aggregationResultHolder.getResult();
    if (sketch == null) {
      sketch = new LongsSketch(_maxMapSize);
      aggregationResultHolder.setValue(sketch);
    }
    return sketch;
  }

  /**
   * Extracts the sketch from the group by result holder for key
   * or creates a new one if it does not exist.
   */
  protected LongsSketch getOrCreateSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    LongsSketch sketch = groupByResultHolder.getResult(groupKey);
    if (sketch == null) {
      sketch = new LongsSketch(_maxMapSize);
      groupByResultHolder.setValueForKey(groupKey, sketch);
    }
    return sketch;
  }

  /**
   * Deserializes the sketches from the bytes.
   */
  protected LongsSketch[] deserializeSketches(byte[][] serializedSketches) {
    LongsSketch[] sketches = new LongsSketch[serializedSketches.length];
    for (int i = 0; i < serializedSketches.length; i++) {
      sketches[i] = LongsSketch.getInstance(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  @Override
  public LongsSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public LongsSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public LongsSketch merge(LongsSketch sketch1, LongsSketch sketch2) {
    LongsSketch union = new LongsSketch(_maxMapSize);
    if (sketch1 != null) {
      union.merge(sketch1);
    }
    if (sketch2 != null) {
      union.merge(sketch2);
    }
    return union;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.STRING;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.FREQUENTLONGSSKETCH.getName().toLowerCase()
        + "(" + _expression + ")";
  }

  @Override
  public Comparable<?> extractFinalResult(LongsSketch sketch) {
    return new SerializedFrequentLongsSketch(sketch);
  }
}
