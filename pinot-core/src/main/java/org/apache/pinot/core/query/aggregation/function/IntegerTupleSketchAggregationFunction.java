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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryDeserializer;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


/// The `IntegerTupleSketchAggregationFunction` is the base class for all integer-based Tuple Sketch aggregations.
/// Apache Datasketches Tuple Sketches are an extension of the Apache Datasketches Theta Sketch. Tuple sketches store an
/// additional summary value with each retained entry which makes the sketch ideal for summarizing attributes
/// such as impressions or clicks.
///
/// Tuple sketches are interoperable with the Theta Sketch and enable set operations over a stream of data, and can
/// also be used for cardinality estimation.
///
/// Note: The current implementation of this aggregation function is limited to binary columns that contain sketches
/// built outside of Pinot.
///
/// Usage examples:
///
/// - Simple union (1 or 2 arguments): main expression to aggregate on, followed by an optional Tuple sketch size
///   argument. The second argument is the nominal entries, and defaults to 16384.
///   The "raw" equivalents return serialised sketches in base64-encoded strings.
///
///   DISTINCT_COUNT_TUPLE_SKETCH(col)
///
///   DISTINCT_COUNT_TUPLE_SKETCH(col, 16384)
///
///   DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col)
///
///   DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col, 16384)
/// - Extracting a cardinality estimate from a CPC sketch:
///
///   GET_INT_TUPLE_SKETCH_ESTIMATE(sketch_bytes)
///
///   GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_TUPLE_SKETCH(col))
/// - Union between two sketches summaries are merged using addition for hash keys in common:
///
///     INT_SUM_TUPLE_SKETCH_UNION(
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
///     )
///
/// - Union between two sketches summaries are merged using maximum for hash keys in common:
///
///     INT_MAX_TUPLE_SKETCH_UNION(
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
///     )
///
/// - Union between two sketches summaries are merged using minimum for hash keys in common:
///
///     INT_MIN_TUPLE_SKETCH_UNION(
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
///      DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
///     )
@SuppressWarnings({"rawtypes"})
public class IntegerTupleSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<TupleIntSketchAccumulator, Comparable> {
  private static final int DEFAULT_ACCUMULATOR_THRESHOLD = 2;
  final ExpressionContext _expressionContext;
  final IntegerSummarySetOperations _setOps;
  protected int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;
  protected int _nominalEntries;

  public IntegerTupleSketchAggregationFunction(List<ExpressionContext> arguments, IntegerSummary.Mode mode,
      boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);

    Preconditions.checkArgument(arguments.size() <= 2,
        "Tuple Sketch Aggregation Function expects at most 2 arguments, got: %s", arguments.size());
    _expressionContext = arguments.get(0);
    _setOps = new IntegerSummarySetOperations(mode, mode);
    if (arguments.size() == 2) {
      ExpressionContext secondArgument = arguments.get(1);
      Preconditions.checkArgument(secondArgument.getType() == ExpressionContext.Type.LITERAL,
          "Tuple Sketch Aggregation Function expects the second argument to be a literal (parameters)," + " but got: ",
          secondArgument.getType());

      if (secondArgument.getLiteral().getType() == DataType.STRING) {
        Parameters parameters = new Parameters(secondArgument.getLiteral().getStringValue());
        // Allows the user to trade-off memory usage for merge CPU; higher values use more memory
        _accumulatorThreshold = parameters.getAccumulatorThreshold();
        // Nominal entries controls sketch accuracy and size
        _nominalEntries = parameters.getNominalEntries();
      } else {
        _nominalEntries = secondArgument.getLiteral().getIntValue();
      }
    } else {
      _nominalEntries = (int) Math.pow(2, CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK);
    }
  }

  // TODO if extra aggregation modes are supported, make this switch
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH;
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
    Preconditions.checkState(dataType == DataType.BYTES && singleValue,
        "INTEGER_TUPLE_SKETCH only supports SV BYTES column");
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    // The accumulator is created inside the range, so an all-null block leaves the holder untouched and
    // extractFinalResult sees the null that means nothing was aggregated
    forEachNotNull(length, blockValSet, (from, to) -> {
      // An empty range still reaches here, for a zero-length block. Creating the accumulator for it would mark
      // the holder as aggregated and lose the signal this whole arrangement exists to carry.
      TupleIntSketchAccumulator tupleIntSketchAccumulator = getAccumulator(aggregationResultHolder);
      for (int i = from; i < to; i++) {
        tupleIntSketchAccumulator.apply(deserializeSketch(bytesValues[i]));
      }
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    Preconditions.checkState(dataType == DataType.BYTES && singleValue,
        "INTEGER_TUPLE_SKETCH only supports SV BYTES column");
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        getAccumulator(groupByResultHolder, groupKeyArray[i]).apply(deserializeSketch(bytesValues[i]));
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    DataType dataType = blockValSet.getValueType();
    boolean singleValue = blockValSet.isSingleValue();
    Preconditions.checkState(dataType == DataType.BYTES && singleValue,
        "INTEGER_TUPLE_SKETCH only supports SV BYTES column");
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        // Deserialized once per row, not once per group key the row belongs to
        TupleSketch<IntegerSummary> sketch = deserializeSketch(bytesValues[i]);
        for (int groupKey : groupKeysArray[i]) {
          getAccumulator(groupByResultHolder, groupKey).apply(sketch);
        }
      }
    });
  }

  @Override
  @Nullable
  public TupleIntSketchAccumulator extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  /// The accumulator an untouched holder stands for, built where the disabled-mode answer is rendered rather than
  /// substituted during extraction.
  TupleIntSketchAccumulator emptyAccumulator() {
    return new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
  }

  @Nullable
  @Override
  public TupleIntSketchAccumulator extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public TupleIntSketchAccumulator merge(TupleIntSketchAccumulator intermediateResult1,
      TupleIntSketchAccumulator intermediateResult2) {
    if (intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    intermediateResult1.setThreshold(_accumulatorThreshold);
    intermediateResult1.setNominalEntries(_nominalEntries);
    intermediateResult1.setSetOperations(_setOps);
    intermediateResult1.merge(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(TupleIntSketchAccumulator tupleIntSketchAccumulator) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.TupleIntSketchAccumulator.getValue(),
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_ACCUMULATOR_SER_DE.serialize(tupleIntSketchAccumulator));
  }

  @Override
  public TupleIntSketchAccumulator deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_ACCUMULATOR_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Nullable
  @Override
  public Comparable extractFinalResult(@Nullable TupleIntSketchAccumulator accumulator) {
    // A null intermediate result means nothing was aggregated. With null handling enabled that is NULL; with it
    // disabled the answer stays what it has always been, the serialized empty sketch.
    if (accumulator == null) {
      if (_nullHandlingEnabled) {
        return null;
      }
      accumulator = emptyAccumulator();
    }
    accumulator.setNominalEntries(_nominalEntries);
    accumulator.setSetOperations(_setOps);
    accumulator.setThreshold(_accumulatorThreshold);
    return Base64.getEncoder().encodeToString(accumulator.getResult().toByteArray());
  }

  @Override
  public boolean canUseStarTree(Map<String, Object> functionParameters) {
    Object nominalEntriesParam = functionParameters.get(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES);
    int starTreeNominalEntries;

    // Check if nominal entries values match
    if (nominalEntriesParam != null) {
      starTreeNominalEntries = Integer.parseInt(String.valueOf(nominalEntriesParam));
    } else {
      // If the functionParameters don't have an explicit nominal entries value set, it means that the star-tree
      // index was built with the default value for nominal entries
      starTreeNominalEntries = (int) Math.pow(2, CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK);
    }
    // Check if the query nominalEntries param is less than or equal to that of the StarTree aggregation.
    // LEQ is used instead of direct equality because it allows the end user to use a single index to serve various
    // query precisions depending on the use case.  Apache Datasketches sketches of higher precision can seamlessly
    // adjust down to lower precision if desired.
    return _nominalEntries <= starTreeNominalEntries;
  }

  /// Returns the accumulator from the result holder or creates a new one if it does not exist.
  private TupleIntSketchAccumulator getAccumulator(AggregationResultHolder aggregationResultHolder) {
    TupleIntSketchAccumulator accumulator = aggregationResultHolder.getResult();
    if (accumulator == null) {
      accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
      aggregationResultHolder.setValue(accumulator);
    }
    return accumulator;
  }

  /// Returns the accumulator for the given group key or creates a new one if it does not exist.
  private TupleIntSketchAccumulator getAccumulator(GroupByResultHolder groupByResultHolder, int groupKey) {
    TupleIntSketchAccumulator accumulator = groupByResultHolder.getResult(groupKey);
    if (accumulator == null) {
      accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
      groupByResultHolder.setValueForKey(groupKey, accumulator);
    }
    return accumulator;
  }

  /// Deserializes a single serialized sketch, so a row that is skipped as null is never heapified.
  private TupleSketch<IntegerSummary> deserializeSketch(byte[] serializedSketch) {
    return TupleSketch.heapifySketch(MemorySegment.ofArray(serializedSketch), new IntegerSummaryDeserializer());
  }

  /// Helper class to wrap the tuple-sketch parameters.  The initial values for the parameters are set to the
  /// same defaults in the Apache Datasketches library.
  private static class Parameters {
    private static final char PARAMETER_DELIMITER = ';';
    private static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';
    private static final String NOMINAL_ENTRIES_KEY = "nominalEntries";
    private static final String ACCUMULATOR_THRESHOLD_KEY = "accumulatorThreshold";

    private int _nominalEntries = (int) Math.pow(2, CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK);
    private int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;

    Parameters(String parametersString) {
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        if (key.equalsIgnoreCase(NOMINAL_ENTRIES_KEY)) {
          _nominalEntries = Integer.parseInt(value);
        } else if (key.equalsIgnoreCase(ACCUMULATOR_THRESHOLD_KEY)) {
          _accumulatorThreshold = Integer.parseInt(value);
        } else {
          throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }

    int getNominalEntries() {
      return _nominalEntries;
    }

    int getAccumulatorThreshold() {
      return _accumulatorThreshold;
    }
  }
}
