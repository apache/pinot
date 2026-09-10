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
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.SerializedFrequentLongsSketch;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


///  `FrequentLongsSketchAggregationFunction` provides an approximate FrequentItems aggregation function based on
///  [Apache DataSketches library](https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html) . It is
///  memory efficient compared to exact counting.
///
///   The function takes an INT or LONG column as input and returns a Base64 encoded sketch object which can be
///   deserialized and used to estimate the frequency of items in the dataset (how many times they appear).
///
/// **FREQUENT_STRINGS_SKETCH(col, maxMapSize=256)**
///
/// E.g.:
///
/// - **FREQUENT_LONGS_SKETCH(col)**
/// - **FREQUENT_LONGS_SKETCH(col, 1024)**
///
///   If the column type is BYTES, the aggregation function will assume it is a serialized FrequentItems data sketch
///   of type `FrequentLongsSketch` and will attempt to deserialize it for merging with other sketch objects.
///
///   Second argument, maxMapsSize, refers to the size of the physical length of the hashmap which stores counts. It
///   influences the accuracy of the sketch and should be a power of 2.
///
///   There is a variation of the function (**FREQUENT_STRINGS_SKETCH**) which accepts STRING type input columns.
public class FrequentLongsSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<FrequentLongsSketch, Comparable<?>> {
  protected static final int DEFAULT_MAX_MAP_SIZE = 256;

  protected int _maxMapSize;

  public FrequentLongsSketchAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
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
    DataType dataType = valueSet.getValueType();
    boolean singleValue = valueSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Assuming the column contains serialized data sketch
      byte[][] bytesValues = valueSet.getBytesValuesSV();
      // The sketch is created inside the range, so a block with no non-null row leaves the holder untouched and
      // extractFinalResult sees the null that means nothing was aggregated
      forEachNotNull(length, valueSet, (from, to) -> {
        FrequentLongsSketch sketch = getOrCreateSketch(aggregationResultHolder);
        for (int i = from; i < to; i++) {
          sketch.merge(deserializeSketch(bytesValues[i]));
        }
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    Preconditions.checkState(storedType == DataType.INT || storedType == DataType.LONG,
        "FREQUENT_LONGS_SKETCH only supports INT/LONG column");
    if (singleValue) {
      long[] longValues = valueSet.getLongValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        FrequentLongsSketch sketch = getOrCreateSketch(aggregationResultHolder);
        for (int i = from; i < to; i++) {
          sketch.update(longValues[i]);
        }
      });
    } else {
      long[][] longValues = valueSet.getLongValuesMV();
      forEachNotNull(length, valueSet, (from, to) -> {
        FrequentLongsSketch sketch = getOrCreateSketch(aggregationResultHolder);
        for (int i = from; i < to; i++) {
          for (long value : longValues[i]) {
            sketch.update(value);
          }
        }
      });
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType dataType = valueSet.getValueType();
    boolean singleValue = valueSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Assuming the column contains serialized data sketch
      byte[][] bytesValues = valueSet.getBytesValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          getOrCreateSketch(groupByResultHolder, groupKeyArray[i]).merge(deserializeSketch(bytesValues[i]));
        }
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    Preconditions.checkState(storedType == DataType.INT || storedType == DataType.LONG,
        "FREQUENT_LONGS_SKETCH only supports INT/LONG column");
    if (singleValue) {
      long[] values = valueSet.getLongValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          getOrCreateSketch(groupByResultHolder, groupKeyArray[i]).update(values[i]);
        }
      });
    } else {
      long[][] values = valueSet.getLongValuesMV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          FrequentLongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
          for (long value : values[i]) {
            sketch.update(value);
          }
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType dataType = valueSet.getValueType();
    boolean singleValue = valueSet.isSingleValue();
    if (dataType == DataType.BYTES && singleValue) {
      // Assuming the column contains serialized data sketch
      byte[][] bytesValues = valueSet.getBytesValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          // Deserialized once per row, not once per group key the row belongs to
          FrequentLongsSketch rowSketch = deserializeSketch(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            getOrCreateSketch(groupByResultHolder, groupKey).merge(rowSketch);
          }
        }
      });
      return;
    }

    DataType storedType = dataType.getStoredType();
    Preconditions.checkState(storedType == DataType.INT || storedType == DataType.LONG,
        "FREQUENT_LONGS_SKETCH only supports INT/LONG column");
    if (singleValue) {
      long[] values = valueSet.getLongValuesSV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getOrCreateSketch(groupByResultHolder, groupKey).update(values[i]);
          }
        }
      });
    } else {
      long[][] values = valueSet.getLongValuesMV();
      forEachNotNull(length, valueSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          long[] rowValues = values[i];
          for (int groupKey : groupKeysArray[i]) {
            FrequentLongsSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
            for (long value : rowValues) {
              sketch.update(value);
            }
          }
        }
      });
    }
  }

  /// Extracts the sketch from the result holder or creates a new one if it does not exist.
  protected FrequentLongsSketch getOrCreateSketch(AggregationResultHolder aggregationResultHolder) {
    FrequentLongsSketch sketch = aggregationResultHolder.getResult();
    if (sketch == null) {
      sketch = new FrequentLongsSketch(_maxMapSize);
      aggregationResultHolder.setValue(sketch);
    }
    return sketch;
  }

  /// Extracts the sketch from the group by result holder for key
  /// or creates a new one if it does not exist.
  protected FrequentLongsSketch getOrCreateSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    FrequentLongsSketch sketch = groupByResultHolder.getResult(groupKey);
    if (sketch == null) {
      sketch = new FrequentLongsSketch(_maxMapSize);
      groupByResultHolder.setValueForKey(groupKey, sketch);
    }
    return sketch;
  }

  /// Deserializes a single serialized sketch, so a row that is skipped as null is never deserialized.
  protected FrequentLongsSketch deserializeSketch(byte[] serializedSketch) {
    return FrequentLongsSketch.getInstance(MemorySegment.ofArray(serializedSketch));
  }

  @Nullable
  @Override
  public FrequentLongsSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public FrequentLongsSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public FrequentLongsSketch merge(FrequentLongsSketch sketch1, FrequentLongsSketch sketch2) {
    FrequentLongsSketch union = new FrequentLongsSketch(_maxMapSize);
    union.merge(sketch1);
    union.merge(sketch2);
    return union;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(FrequentLongsSketch longsSketch) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.FrequentLongsSketch.getValue(),
        ObjectSerDeUtils.FREQUENT_LONGS_SKETCH_SER_DE.serialize(longsSketch));
  }

  @Override
  public FrequentLongsSketch deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.FREQUENT_LONGS_SKETCH_SER_DE.deserialize(customObject.getBuffer());
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

  @Nullable
  @Override
  public Comparable<?> extractFinalResult(@Nullable FrequentLongsSketch sketch) {
    // A null intermediate result means nothing was aggregated, and there is no sketch to serialize. This function
    // has never substituted an empty accumulator during extraction, so NULL is the answer in both modes and there is
    // no disabled-mode identity to preserve here.
    return sketch != null ? new SerializedFrequentLongsSketch(sketch) : null;
  }
}
