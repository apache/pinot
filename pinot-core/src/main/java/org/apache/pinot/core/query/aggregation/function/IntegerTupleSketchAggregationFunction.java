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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants;


/***
 * This is the base class for all Integer Tuple Sketch aggregations
 *
 * Note that it only supports BYTES columns containing serialized sketches currently, but could be expanded to more
 */
public class IntegerTupleSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<List<CompactSketch<IntegerSummary>>, Comparable> {
  final ExpressionContext _expressionContext;
  final IntegerSummarySetOperations _setOps;
  final int _entries;

  public IntegerTupleSketchAggregationFunction(List<ExpressionContext> arguments, IntegerSummary.Mode mode) {
    super(arguments.get(0));

    Preconditions.checkArgument(arguments.size() <= 2,
        "Tuple Sketch Aggregation Function expects at most 2 arguments, got: %s", arguments.size());
    _expressionContext = arguments.get(0);
    _setOps = new IntegerSummarySetOperations(mode, mode);
    if (arguments.size() == 2) {
      ExpressionContext secondArgument = arguments.get(1);
      Preconditions.checkArgument(secondArgument.getType() == ExpressionContext.Type.LITERAL,
          "Tuple Sketch Aggregation Function expects the second argument to be a literal (number of entries to keep),"
              + " but got: ", secondArgument.getType());
      _entries = secondArgument.getLiteral().getIntValue();
    } else {
      _entries = (int) Math.pow(2, CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK);
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
        List<CompactSketch<IntegerSummary>> integerSketch = aggregationResultHolder.getResult();
        if (integerSketch != null) {
          List<CompactSketch<IntegerSummary>> sketches =
              Arrays.stream(bytesValues).map(ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE::deserialize)
                  .map(Sketch::compact).collect(Collectors.toList());
          aggregationResultHolder.setValue(merge(aggregationResultHolder.getResult(), sketches));
        } else {
          List<CompactSketch<IntegerSummary>> sketches =
              Arrays.stream(bytesValues).map(ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE::deserialize)
                  .map(Sketch::compact).collect(Collectors.toList());
          aggregationResultHolder.setValue(sketches);
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging Tuple Sketches", e);
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
        for (int i = 0; i < length; i++) {
          byte[] value = bytesValues[i];
          int groupKey = groupKeyArray[i];
          CompactSketch<IntegerSummary> newSketch =
              ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(value).compact();
          if (groupByResultHolder.getResult(groupKey) == null) {
            ArrayList<CompactSketch<IntegerSummary>> newList = new ArrayList<>();
            newList.add(newSketch);
            groupByResultHolder.setValueForKey(groupKey, newList);
          } else {
            groupByResultHolder.<List<CompactSketch<IntegerSummary>>>getResult(groupKey).add(newSketch);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging Tuple Sketches", e);
      }
    } else {
      throw new IllegalStateException(
          "Illegal data type for INTEGER_TUPLE_SKETCH_UNION aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] valueArray = blockValSetMap.get(_expression).getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      byte[] value = valueArray[i];
      CompactSketch<IntegerSummary> newSketch =
          ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(value).compact();
      for (int groupKey : groupKeysArray[i]) {
        if (groupByResultHolder.getResult(groupKey) == null) {
          groupByResultHolder.setValueForKey(groupKey, Collections.singletonList(newSketch));
        } else {
          groupByResultHolder.<List<CompactSketch<IntegerSummary>>>getResult(groupKey).add(newSketch);
        }
      }
    }
  }

  @Override
  public List<CompactSketch<IntegerSummary>> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public List<CompactSketch<IntegerSummary>> extractGroupByResult(GroupByResultHolder groupByResultHolder,
      int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public List<CompactSketch<IntegerSummary>> merge(List<CompactSketch<IntegerSummary>> intermediateResult1,
      List<CompactSketch<IntegerSummary>> intermediateResult2) {
    if (intermediateResult1 == null && intermediateResult2 != null) {
      return intermediateResult2;
    } else if (intermediateResult1 != null && intermediateResult2 == null) {
      return intermediateResult1;
    } else if (intermediateResult1 == null && intermediateResult2 == null) {
      return new ArrayList<>(0);
    }
    ArrayList<CompactSketch<IntegerSummary>> merged =
        new ArrayList<>(intermediateResult1.size() + intermediateResult2.size());
    merged.addAll(intermediateResult1);
    merged.addAll(intermediateResult2);
    return merged;
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
  public Comparable extractFinalResult(List<CompactSketch<IntegerSummary>> integerSummarySketches) {
    if (integerSummarySketches == null) {
      return null;
    }
    Union<IntegerSummary> union = new Union<>(_entries, _setOps);
    integerSummarySketches.forEach(union::union);
    return Base64.getEncoder().encodeToString(union.getResult().toByteArray());
  }
}
