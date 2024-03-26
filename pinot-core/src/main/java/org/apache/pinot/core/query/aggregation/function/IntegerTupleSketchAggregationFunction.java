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
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryDeserializer;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * The {@code IntegerTupleSketchAggregationFunction} is the base class for all integer-based Tuple Sketch aggregations.
 * Apache Datasketches Tuple Sketches are an extension of the Apache Datasketches Theta Sketch. Tuple sketches store an
 * additional summary value with each retained entry which makes the sketch ideal for summarizing attributes
 * such as impressions or clicks.
 *
 * Tuple sketches are interoperable with the Theta Sketch and enable set operations over a stream of data, and can
 * also be used for cardinality estimation.
 *
 * Note: The current implementation of this aggregation function is limited to binary columns that contain sketches
 * built outside of Pinot.
 *
 * Usage examples:
 * <ul>
 *   <li>
 *     Simple union (1 or 2 arguments): main expression to aggregate on, followed by an optional Tuple sketch size
 *     argument. The second argument is the sketch lgK – the given log_base2 of k, and defaults to 16.
 *     The "raw" equivalents return serialised sketches in base64-encoded strings.
 *     <p>DISTINCT_COUNT_TUPLE_SKETCH(col)</p>
 *     <p>DISTINCT_COUNT_TUPLE_SKETCH(col, 12)</p>
 *     <p>DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col)</p>
 *     <p>DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col, 12)</p>
 *   <li>
 *     Extracting a cardinality estimate from a CPC sketch:
 *     <p>GET_INT_TUPLE_SKETCH_ESTIMATE(sketch_bytes)</p>
 *     <p>GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_TUPLE_SKETCH(col))</p>
 *   </li>
 *   <li>
 *     Union between two sketches summaries are merged using addition for hash keys in common:
 *     <p>
 *       INT_SUM_TUPLE_SKETCH_UNION(
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
 *       )
 *     </p>
 *   </li>
 *   <li>
 *     Union between two sketches summaries are merged using maximum for hash keys in common:
 *     <p>
 *       INT_MAX_TUPLE_SKETCH_UNION(
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
 *       )
 *     </p>
 *   </li>
 *   <li>
 *     Union between two sketches summaries are merged using minimum for hash keys in common:
 *     <p>
 *       INT_MIN_TUPLE_SKETCH_UNION(
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col1),
 *         DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(col2)
 *       )
 *     </p>
 *  </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes"})
public class IntegerTupleSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<TupleIntSketchAccumulator, Comparable> {
  private static final int DEFAULT_ACCUMULATOR_THRESHOLD = 2;
  final ExpressionContext _expressionContext;
  final IntegerSummarySetOperations _setOps;
  protected int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;
  protected int _nominalEntries;

  public IntegerTupleSketchAggregationFunction(List<ExpressionContext> arguments, IntegerSummary.Mode mode) {
    super(arguments.get(0));

    Preconditions.checkArgument(arguments.size() <= 2,
        "Tuple Sketch Aggregation Function expects at most 2 arguments, got: %s", arguments.size());
    _expressionContext = arguments.get(0);
    _setOps = new IntegerSummarySetOperations(mode, mode);
    if (arguments.size() == 2) {
      ExpressionContext secondArgument = arguments.get(1);
      Preconditions.checkArgument(secondArgument.getType() == ExpressionContext.Type.LITERAL,
          "Tuple Sketch Aggregation Function expects the second argument to be a literal (parameters)," + " but got: ",
          secondArgument.getType());

      if (secondArgument.getLiteral().getType() == FieldSpec.DataType.STRING) {
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

    // Treat BYTES value as serialized Integer Tuple Sketch
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        TupleIntSketchAccumulator tupleIntSketchAccumulator = getAccumulator(aggregationResultHolder);
        Sketch<IntegerSummary>[] sketches = deserializeSketches(bytesValues, length);
        for (Sketch<IntegerSummary> sketch : sketches) {
          tupleIntSketchAccumulator.apply(sketch);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while aggregating Tuple Sketches", e);
      }
    } else {
      throw new IllegalStateException("Illegal data type for " + getType() + " aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {

    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized Integer Tuple Sketch
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();

    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        Sketch<IntegerSummary>[] sketches = deserializeSketches(bytesValues, length);
        for (int i = 0; i < length; i++) {
          TupleIntSketchAccumulator tupleIntSketchAccumulator = getAccumulator(groupByResultHolder, groupKeyArray[i]);
          Sketch<IntegerSummary> sketch = sketches[i];
          tupleIntSketchAccumulator.apply(sketch);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while aggregating Tuple Sketches", e);
      }
    } else {
      throw new IllegalStateException(
          "Illegal data type for INTEGER_TUPLE_SKETCH_UNION aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized Integer Tuple Sketch
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    boolean singleValue = blockValSet.isSingleValue();

    if (singleValue && storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSetMap.get(_expression).getBytesValuesSV();
      try {
        Sketch<IntegerSummary>[] sketches = deserializeSketches(bytesValues, length);
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getAccumulator(groupByResultHolder, groupKey).apply(sketches[i]);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while aggregating Tuple Sketches", e);
      }
    } else {
      throw new IllegalStateException(
          "Illegal data type for INTEGER_TUPLE_SKETCH_UNION aggregation function: " + storedType);
    }
  }

  @Override
  public TupleIntSketchAccumulator extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    TupleIntSketchAccumulator result = aggregationResultHolder.getResult();
    if (result == null) {
      return new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
    }
    return result;
  }

  @Override
  public TupleIntSketchAccumulator extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public TupleIntSketchAccumulator merge(TupleIntSketchAccumulator intermediateResult1,
      TupleIntSketchAccumulator intermediateResult2) {
    if (intermediateResult1 == null || intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null || intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    intermediateResult1.merge(intermediateResult2);
    return intermediateResult1;
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
  public Comparable extractFinalResult(TupleIntSketchAccumulator accumulator) {
    accumulator.setNominalEntries(_nominalEntries);
    accumulator.setSetOperations(_setOps);
    accumulator.setThreshold(_accumulatorThreshold);
    return Base64.getEncoder().encodeToString(accumulator.getResult().toByteArray());
  }

  /**
   * Returns the accumulator from the result holder or creates a new one if it does not exist.
   */
  private TupleIntSketchAccumulator getAccumulator(AggregationResultHolder aggregationResultHolder) {
    TupleIntSketchAccumulator accumulator = aggregationResultHolder.getResult();
    if (accumulator == null) {
      accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
      aggregationResultHolder.setValue(accumulator);
    }
    return accumulator;
  }

  /**
   * Returns the accumulator for the given group key or creates a new one if it does not exist.
   */
  private TupleIntSketchAccumulator getAccumulator(GroupByResultHolder groupByResultHolder, int groupKey) {
    TupleIntSketchAccumulator accumulator = groupByResultHolder.getResult(groupKey);
    if (accumulator == null) {
      accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, _accumulatorThreshold);
      groupByResultHolder.setValueForKey(groupKey, accumulator);
    }
    return accumulator;
  }

  /**
   * Deserializes the sketches from the bytes.
   */
  @SuppressWarnings({"unchecked"})
  private Sketch<IntegerSummary>[] deserializeSketches(byte[][] serializedSketches, int length) {
    Sketch<IntegerSummary>[] sketches = new Sketch[length];
    for (int i = 0; i < length; i++) {
      sketches[i] = Sketches.heapifySketch(Memory.wrap(serializedSketches[i]), new IntegerSummaryDeserializer());
    }
    return sketches;
  }

  /**
   * Helper class to wrap the tuple-sketch parameters.  The initial values for the parameters are set to the
   * same defaults in the Apache Datasketches library.
   */
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
