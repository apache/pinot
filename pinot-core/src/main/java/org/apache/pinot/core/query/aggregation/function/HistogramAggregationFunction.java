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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.DoubleVectorOpUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Histogram for single-value numerical columns
 * usage example:
 * `Histogram(columnName, ARRAY[0,1,10,100])` to specify bins [0,1), [1,10), [10,1000] or
 * `Histogram(columnName, 0, 1000, 10)` to specify 10 equal-length bins [0,100), [100,200), ..., [900,1000]
 */
public class HistogramAggregationFunction extends BaseSingleInputAggregationFunction<DoubleArrayList, DoubleArrayList> {

  private static final String ARRAY_CONSTRUCTOR = "arrayvalueconstructor";
  private static final int INVALID_BIN = -1;
  double[] _bucketEdges;
  boolean _isEqualLength = false;
  double _lower;
  double _upper;
  double _binLength;

  public HistogramAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 4 || numArguments == 2, "Histogram expects 2 or 4 arguments, got: %s;"
        + " usage example: `Histogram(columnName, ARRAY[0,1,10,100])` to specify bins [0,1), [1,10), [10,1000] or "
        + "`Histogram(columnName, 0, 1000, 10)` to specify 10 equal-length bins "
        + "[0,100), [100,200), ..., [900,1000]", numArguments);
    if (numArguments == 2) {
      ExpressionContext arrayExpression = arguments.get(1);
      Preconditions.checkArgument(
          // ARRAY function
          ((arrayExpression.getType() == ExpressionContext.Type.FUNCTION)
              && (arrayExpression.getFunction().getFunctionName().equals(ARRAY_CONSTRUCTOR)))
              || ((arrayExpression.getType() == ExpressionContext.Type.LITERAL)
              && (arrayExpression.getLiteral().getValue() instanceof List)),
          "Please use the format of `Histogram(columnName, ARRAY[1,10,100])` to specify the bin edges");
      if (arrayExpression.getType() == ExpressionContext.Type.FUNCTION) {
        _bucketEdges = parseVector(arrayExpression.getFunction().getArguments());
      } else {
        _bucketEdges = parseVectorLiteral((List) arrayExpression.getLiteral().getValue());
      }
      _lower = _bucketEdges[0];
      _upper = _bucketEdges[_bucketEdges.length - 1];
    } else {
      _isEqualLength = true;
      _lower = arguments.get(1).getLiteral().getDoubleValue();
      _upper = arguments.get(2).getLiteral().getDoubleValue();
      int numBins = arguments.get(3).getLiteral().getIntValue();
      ;
      Preconditions.checkArgument(_upper > _lower,
          "The right most edge must be greater than left most edge, given %s and %s", _lower, _upper);
      Preconditions.checkArgument(numBins > 0, "The number of bins must be greater than zero, given %s", numBins);
      _bucketEdges = new double[numBins + 1];
      _bucketEdges[0] = _lower;
      _bucketEdges[numBins] = _upper;
      _binLength = (_upper - _lower) / numBins;
      for (int i = 1; i < numBins; i++) {
        _bucketEdges[i] = i * _binLength + _lower;
      }
    }
  }

  int getNumBins() {
    return _bucketEdges.length - 1;
  }

  int getNumEdges() {
    return _bucketEdges.length;
  }

  private double[] parseVector(List<ExpressionContext> arrayStr) {
    int len = arrayStr.size();
    Preconditions.checkArgument(len > 1, "The number of bin edges must be greater than 1");
    double[] ret = new double[len];
    for (int i = 0; i < len; i++) {
      // TODO: Represent infinity as literal instead of identifier
      if (arrayStr.get(i).getType() == ExpressionContext.Type.IDENTIFIER) {
        ret[i] = Double.parseDouble(arrayStr.get(i).getIdentifier());
      } else {
        ret[i] = arrayStr.get(i).getLiteral().getDoubleValue();
      }
      if (i > 0) {
        Preconditions.checkState(ret[i] > ret[i - 1], "The bin edges must be strictly increasing");
      }
    }
    return ret;
  }

  private double[] parseVectorLiteral(List arrayStr) {
    int len = arrayStr.size();
    Preconditions.checkArgument(len > 1, "The number of bin edges must be greater than 1");
    double[] ret = new double[len];
    for (int i = 0; i < len; i++) {
      // TODO: Represent infinity as literal instead of identifier
      ret[i] = Double.parseDouble(arrayStr.get(i).toString());
      if (i > 0) {
        Preconditions.checkState(ret[i] > ret[i - 1], "The bin edges must be strictly increasing");
      }
    }
    return ret;
  }

  /**
   * Find the bin id for the input value. Use division for equal-length bins, and binary search otherwise.
   *
   * @param val input value
   * @return bin id
   */
  private int getBinId(double val) {
    if (val > _upper || val < _lower) {
      return INVALID_BIN;
    }
    if (val == _upper) {
      return getNumBins() - 1;
    }
    int id;
    if (_isEqualLength) {
      id = (int) Math.floor((val - _lower) / _binLength);
    } else {
      int i = 0;
      int j = this.getNumEdges() - 1;
      while (i < j) {
        int mid = (i + j + 1) / 2;
        if (_bucketEdges[mid] > val) {
          j = mid - 1;
        } else {
          i = mid;
        }
      }
      id = i;
    }
    return id;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.HISTOGRAM;
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
  public DoubleArrayList extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    DoubleArrayList aggregationResultHolderResult = aggregationResultHolder.getResult();
    if (aggregationResultHolderResult == null) {
      return DoubleVectorOpUtils.createAndInitialize(getNumBins());
    } else {
      return aggregationResultHolderResult;
    }
  }

  @Override
  public DoubleArrayList extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public DoubleArrayList merge(DoubleArrayList intermediateResult1, DoubleArrayList intermediateResult2) {
    DoubleVectorOpUtils.vectorAdd(intermediateResult1, intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE_ARRAY;
  }

  @Override
  public DoubleArrayList extractFinalResult(DoubleArrayList doubleArrayList) {
    int count = doubleArrayList.size();
    if (count < 1L) {
      throw new IllegalStateException("histogram result shouldn't be empty!");
    } else {
      return new DoubleArrayList(doubleArrayList.elements());
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    Preconditions.checkState(blockValSet.isSingleValue(), "Histogram currently only supports single-valued column");
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          double value = values[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          double value = values[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          double value = values[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          double value = values[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: " + blockValSet.getValueType());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, values[i]);
        }
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, values[i]);
        }
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, values[i]);
        }
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, values[i]);
        }
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: " + blockValSet.getValueType());
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double val) {
    int binID = getBinId(val);
    DoubleArrayList byResultHolderResult = groupByResultHolder.getResult(groupKey);
    if (byResultHolderResult == null) {
      byResultHolderResult = DoubleVectorOpUtils.createAndInitialize(getNumBins());
      groupByResultHolder.setValueForKey(groupKey, byResultHolderResult);
    }
    if (binID != INVALID_BIN) {
      DoubleVectorOpUtils.incrementElementByOne(byResultHolderResult, binID);
    }
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    //TODO: Add MV support for histogram
    Preconditions.checkState(blockValSet.isSingleValue(), "Histogram currently only supports single-valued column");
    double[] histogram = new double[this.getNumBins()];
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          int binId = this.getBinId(values[i]);
          if (binId != INVALID_BIN) {
            histogram[binId] += 1;
          }
        }
        setAggregationResult(aggregationResultHolder, histogram);
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          int binId = this.getBinId(values[i]);
          if (binId != INVALID_BIN) {
            histogram[binId] += 1;
          }
        }
        setAggregationResult(aggregationResultHolder, histogram);
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          int binId = this.getBinId(values[i]);
          if (binId != INVALID_BIN) {
            histogram[binId] += 1;
          }
        }
        setAggregationResult(aggregationResultHolder, histogram);
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length && i < values.length; i++) {
          int binId = this.getBinId(values[i]);
          if (binId != INVALID_BIN) {
            histogram[binId] += 1;
          }
        }
        setAggregationResult(aggregationResultHolder, histogram);
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: " + blockValSet.getValueType());
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double[] histogram) {
    DoubleArrayList aggregatedHistogram = aggregationResultHolder.getResult();
    if (aggregatedHistogram == null) {
      aggregationResultHolder.setValue(DoubleVectorOpUtils.createAndInitialize(histogram));
    } else {
      DoubleVectorOpUtils.vectorAdd(aggregatedHistogram, histogram);
    }
  }
}
