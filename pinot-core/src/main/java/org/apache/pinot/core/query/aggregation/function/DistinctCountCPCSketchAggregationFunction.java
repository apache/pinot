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
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctCountCPCSketchAggregationFunction} is used for space-efficient cardinality estimation.
 * The Apache Datasketches CPC sketch is a unique-counting sketch that implements the
 * <i>Compressed Probabilistic Counting (CPC, a.k.a FM85)</i> algorithms developed by Kevin Lang in his paper
 * <a href="https://arxiv.org/abs/1708.06839">Back to the Future: an Even More Nearly Optimal Cardinality Estimation
 * Algorithm</a>.
 * <br><br>
 * The stored CPC sketch can consume about 40% less space than an HLL sketch of comparable accuracy. CPC sketches have
 * been intentionally designed to offer different tradeoffs to HLL sketches so that, they complement each
 * other in many ways.  For more information, see the Apache Datasketches documentation.
 * <br><br>
 * The aggregation function supports both pre-aggregated sketches or raw values, but no post-aggregation is supported.
 * Usage examples:
 * <ul>
 *   <li>
 *     Simple union (1 or 2 arguments): main expression to aggregate on, followed by an optional CPC sketch size
 *     argument. The second argument is the sketch lgK – the given log_base2 of k, and defaults to 12.
 *     The "raw" equivalents return serialised sketches in base64-encoded strings.
 *     <p>DISTINCT_COUNT_CPC_SKETCH(col)</p>
 *     <p>DISTINCT_COUNT_CPC_SKETCH(col, 12)</p>
 *     <p>DISTINCT_COUNT_RAW_CPC_SKETCH(col)</p>
 *     <p>DISTINCT_COUNT_RAW_CPC_SKETCH(col, 12)</p>
 *   <li>
 *     Extracting a cardinality estimate from a CPC sketch:
 *     <p>GET_CPC_SKETCH_ESTIMATE(sketch_bytes)</p>
 *     <p>GET_CPC_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_CPC_SKETCH(col))</p>
 *   </li>
 *   <li>
 *     Union between two sketches:
 *     <p>
 *       CPC_SKETCH_UNION(
 *         DISTINCT_COUNT_RAW_CPC_SKETCH(col1),
 *         DISTINCT_COUNT_RAW_CPC_SKETCH(col2)
 *       )
 *     </p>
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes"})
public class DistinctCountCPCSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<CpcSketchAccumulator, Comparable> {
  private static final int DEFAULT_ACCUMULATOR_THRESHOLD = 2;
  protected int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;
  protected int _lgNominalEntries;

  public DistinctCountCPCSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 2 arguments - it is a code smell to extend the base for single
    // input aggregation functions.  Nevertheless, there are other functions in the base class that
    // are apply here.  See also: Theta sketch aggregation function.
    Preconditions.checkArgument(numExpressions <= 2, "DistinctCountCPC expects 1 or 2 arguments, got: %s",
        numExpressions);
    if (arguments.size() == 2) {
      ExpressionContext secondArgument = arguments.get(1);
      Preconditions.checkArgument(secondArgument.getType() == ExpressionContext.Type.LITERAL,
          "CPC Sketch Aggregation Function expects the second argument to be a literal (parameters)," + " but got: ",
          secondArgument.getType());

      if (secondArgument.getLiteral().getType() == FieldSpec.DataType.STRING) {
        Parameters parameters = new Parameters(secondArgument.getLiteral().getStringValue());
        // Allows the user to trade-off memory usage for merge CPU; higher values use more memory
        _accumulatorThreshold = parameters.getAccumulatorThreshold();
        // Nominal entries controls sketch accuracy and size
        _lgNominalEntries = parameters.getLgNominalEntries();
      } else {
        _lgNominalEntries = secondArgument.getLiteral().getIntValue();
      }
    } else {
      _lgNominalEntries = CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
    }
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
        CpcSketchAccumulator cpcSketchAccumulator = getAccumulator(aggregationResultHolder);
        CpcSketch[] sketches = deserializeSketches(bytesValues, length);
        for (CpcSketch sketch : sketches) {
          cpcSketchAccumulator.apply(sketch);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging CPC sketches", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
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
    CpcSketchAccumulator cpcSketchAccumulator = getAccumulator(aggregationResultHolder);
    cpcSketchAccumulator.apply(cpcSketch);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized CPC Sketch
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        CpcSketch[] sketches = deserializeSketches(bytesValues, length);
        for (int i = 0; i < length; i++) {
          CpcSketchAccumulator cpcSketchAccumulator = getAccumulator(groupByResultHolder, groupKeyArray[i]);
          CpcSketch sketch = sketches[i];
          cpcSketchAccumulator.apply(sketch);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while aggregating CPC Sketches", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
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
    boolean singleValue = blockValSet.isSingleValue();

    if (singleValue && storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        CpcSketch[] sketches = deserializeSketches(bytesValues, length);
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getAccumulator(groupByResultHolder, groupKey).apply(sketches[i]);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while aggregating CPC sketches", e);
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
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
  public CpcSketchAccumulator extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new CpcSketchAccumulator(_lgNominalEntries, _accumulatorThreshold);
    }

    if (result instanceof CpcSketch) {
      return convertSketchAccumulator(result);
    } else if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to CpcSketch
      return convertSketchAccumulator(dictionaryToCpcSketch((DictIdsWrapper) result));
    } else {
      return (CpcSketchAccumulator) result;
    }
  }

  @Override
  public CpcSketchAccumulator extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new CpcSketchAccumulator(_lgNominalEntries, _accumulatorThreshold);
    }

    if (result instanceof CpcSketch) {
      return convertSketchAccumulator(result);
    } else if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to CpcSketch
      return convertSketchAccumulator(dictionaryToCpcSketch((DictIdsWrapper) result));
    } else {
      return (CpcSketchAccumulator) result;
    }
  }

  @Override
  public CpcSketchAccumulator merge(CpcSketchAccumulator intermediateResult1,
      CpcSketchAccumulator intermediateResult2) {
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
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Comparable extractFinalResult(CpcSketchAccumulator intermediateResult) {
    intermediateResult.setLgNominalEntries(_lgNominalEntries);
    intermediateResult.setThreshold(_accumulatorThreshold);
    return Math.round(intermediateResult.getResult().getEstimate());
  }

  /**
   * Returns the CpcSketch from the result holder or creates a new one if it does not exist.
   */
  protected CpcSketch getCpcSketch(AggregationResultHolder aggregationResultHolder) {
    CpcSketch cpcSketch = aggregationResultHolder.getResult();
    if (cpcSketch == null) {
      cpcSketch = new CpcSketch(_lgNominalEntries);
      aggregationResultHolder.setValue(cpcSketch);
    }
    return cpcSketch;
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the CpcSketch for the given group key or creates a new one if it does not exist.
   */
  protected CpcSketch getCpcSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    CpcSketch cpcSketch = groupByResultHolder.getResult(groupKey);
    if (cpcSketch == null) {
      cpcSketch = new CpcSketch(_lgNominalEntries);
      groupByResultHolder.setValueForKey(groupKey, cpcSketch);
    }
    return cpcSketch;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  private CpcSketch dictionaryToCpcSketch(DictIdsWrapper dictIdsWrapper) {
    CpcSketch cpcSketch = new CpcSketch(_lgNominalEntries);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      Object value = dictionary.get(iterator.next());
      addObjectToSketch(value, cpcSketch);
    }
    return cpcSketch;
  }

  private void addObjectToSketch(Object rawValue, CpcSketch sketch) {
    if (rawValue instanceof String) {
      sketch.update((String) rawValue);
    } else if (rawValue instanceof Integer) {
      sketch.update((Integer) rawValue);
    } else if (rawValue instanceof Long) {
      sketch.update((Long) rawValue);
    } else if (rawValue instanceof Double) {
      sketch.update((Double) rawValue);
    } else if (rawValue instanceof Float) {
      sketch.update((Float) rawValue);
    } else if (rawValue instanceof Object[]) {
      addObjectsToSketch((Object[]) rawValue, sketch);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + rawValue.getClass().getSimpleName());
    }
  }

  private void addObjectsToSketch(Object[] rawValues, CpcSketch sketch) {
    if (rawValues instanceof String[]) {
      for (String s : (String[]) rawValues) {
        sketch.update(s);
      }
    } else if (rawValues instanceof Integer[]) {
      for (Integer i : (Integer[]) rawValues) {
        sketch.update(i);
      }
    } else if (rawValues instanceof Long[]) {
      for (Long l : (Long[]) rawValues) {
        sketch.update(l);
      }
    } else if (rawValues instanceof Double[]) {
      for (Double d : (Double[]) rawValues) {
        sketch.update(d);
      }
    } else if (rawValues instanceof Float[]) {
      for (Float f : (Float[]) rawValues) {
        sketch.update(f);
      }
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + rawValues.getClass().getSimpleName());
    }
  }

  /**
   * Returns the accumulator from the result holder or creates a new one if it does not exist.
   */
  private CpcSketchAccumulator getAccumulator(AggregationResultHolder aggregationResultHolder) {
    CpcSketchAccumulator accumulator = aggregationResultHolder.getResult();
    if (accumulator == null) {
      accumulator = new CpcSketchAccumulator(_lgNominalEntries, _accumulatorThreshold);
      aggregationResultHolder.setValue(accumulator);
    }
    return accumulator;
  }

  /**
   * Returns the accumulator for the given group key or creates a new one if it does not exist.
   */
  private CpcSketchAccumulator getAccumulator(GroupByResultHolder groupByResultHolder, int groupKey) {
    CpcSketchAccumulator accumulator = groupByResultHolder.getResult(groupKey);
    if (accumulator == null) {
      accumulator = new CpcSketchAccumulator(_lgNominalEntries, _accumulatorThreshold);
      groupByResultHolder.setValueForKey(groupKey, accumulator);
    }
    return accumulator;
  }

  /**
   * Deserializes the sketches from the bytes.
   */
  @SuppressWarnings({"unchecked"})
  private CpcSketch[] deserializeSketches(byte[][] serializedSketches, int length) {
    CpcSketch[] sketches = new CpcSketch[length];
    for (int i = 0; i < length; i++) {
      sketches[i] = CpcSketch.heapify(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  // This ensures backward compatibility with servers that still return sketches directly.
  // The AggregationDataTableReducer casts intermediate results to Objects and although the code compiles,
  // types might still be incompatible at runtime due to type erasure.
  // Due to performance overheads of redundant casts, this should be removed at some future point.
  protected CpcSketchAccumulator convertSketchAccumulator(Object result) {
    if (result instanceof CpcSketch) {
      CpcSketch sketch = (CpcSketch) result;
      CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, _accumulatorThreshold);
      accumulator.apply(sketch);
      return accumulator;
    }
    return (CpcSketchAccumulator) result;
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }

  /**
   * Helper class to wrap the CpcSketch parameters.  The initial values for the parameters are set to the
   * same defaults in the Apache Datasketches library.
   */
  private static class Parameters {
    private static final char PARAMETER_DELIMITER = ';';
    private static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';
    private static final String NOMINAL_ENTRIES_KEY = "nominalEntries";
    private static final String ACCUMULATOR_THRESHOLD_KEY = "accumulatorThreshold";

    private int _nominalEntries = (int) Math.pow(2, CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
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

    int getLgNominalEntries() {
      return org.apache.datasketches.common.Util.exactLog2OfInt(_nominalEntries);
    }

    int getAccumulatorThreshold() {
      return _accumulatorThreshold;
    }
  }
}
