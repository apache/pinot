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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.DoubleVectorOpUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


/// Histogram for single-value numerical columns
/// usage example:
/// `Histogram(columnName, ARRAY\[0,1,10,100\])` to specify bins \[0,1), \[1,10), \[10,1000\] or
/// `Histogram(columnName, 0, 1000, 10)` to specify 10 equal-length bins \[0,100), \[100,200), ..., \[900,1000\]
public class HistogramAggregationFunction extends BaseSingleInputAggregationFunction<DoubleArrayList, DoubleArrayList> {

  private static final String ARRAY_CONSTRUCTOR = "arrayvalueconstructor";
  private static final int INVALID_BIN = -1;
  double[] _bucketEdges;
  boolean _isEqualLength = false;
  double _lower;
  double _upper;
  double _binLength;

  public HistogramAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 4 || numArguments == 2, "Histogram expects 2 or 4 arguments, got: %s;"
        + " usage example: `Histogram(columnName, ARRAY[0,1,10,100])` to specify bins [0,1), [1,10), [10,1000] or "
        + "`Histogram(columnName, 0, 1000, 10)` to specify 10 equal-length bins "
        + "[0,100), [100,200), ..., [900,1000]", numArguments);
    if (numArguments == 2) {
      ExpressionContext arrayExpression = arguments.get(1);
      Preconditions.checkArgument(
          // ARRAY function
          (arrayExpression.getType() == ExpressionContext.Type.FUNCTION && arrayExpression.getFunction()
              .getFunctionName().equals(ARRAY_CONSTRUCTOR)) || (
              arrayExpression.getType() == ExpressionContext.Type.LITERAL && !arrayExpression.getLiteral()
                  .isSingleValue()),
          "Please use the format of `Histogram(columnName, ARRAY[1,10,100])` to specify the bin edges");
      if (arrayExpression.getType() == ExpressionContext.Type.FUNCTION) {
        _bucketEdges = parseVector(arrayExpression.getFunction().getArguments());
      } else {
        _bucketEdges = parseVectorLiteral(arrayExpression.getLiteral().getValue());
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
        Preconditions.checkArgument(ret[i] > ret[i - 1], "The bin edges must be strictly increasing");
      }
    }
    return ret;
  }

  private double[] parseVectorLiteral(Object array) {
    Preconditions.checkArgument(array != null, "The bin edges must not be null");
    double[] ret;
    if (array instanceof int[]) {
      int[] intArray = (int[]) array;
      ret = new double[intArray.length];
      ArrayCopyUtils.copy(intArray, ret, intArray.length);
    } else if (array instanceof long[]) {
      long[] longArray = (long[]) array;
      ret = new double[longArray.length];
      ArrayCopyUtils.copy(longArray, ret, longArray.length);
    } else if (array instanceof float[]) {
      float[] floatArray = (float[]) array;
      ret = new double[floatArray.length];
      ArrayCopyUtils.copy(floatArray, ret, floatArray.length);
    } else if (array instanceof double[]) {
      ret = (double[]) array;
    } else {
      throw new IllegalArgumentException("Unsupported array type: " + array.getClass());
    }
    Preconditions.checkArgument(ret.length > 1, "The number of bin edges must be greater than 1");
    for (int i = 1; i < ret.length; i++) {
      Preconditions.checkArgument(ret[i] > ret[i - 1], "The bin edges must be strictly increasing");
    }
    return ret;
  }

  /// Counts one value into the supplied histogram, ignoring values that fall outside every bin.
  private void increment(double[] histogram, double value) {
    int binId = getBinId(value);
    if (binId != INVALID_BIN) {
      histogram[binId] += 1;
    }
  }

  /// Find the bin id for the input value. Use division for equal-length bins, and binary search otherwise.
  ///
  /// @param val input value
  /// @return bin id
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

  @Nullable
  @Override
  public DoubleArrayList extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    DoubleArrayList histogram = aggregationResultHolder.getResult();
    if (histogram != null) {
      return histogram;
    }
    // With the option disabled an untouched holder still renders the empty accumulator, which is the
    // intermediate this mode has always emitted; with it enabled the null is the signal that nothing was
    // aggregated.
    return _nullHandlingEnabled ? null : DoubleVectorOpUtils.createAndInitialize(getNumBins());
  }

  @Nullable
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
  public SerializedIntermediateResult serializeIntermediateResult(DoubleArrayList doubles) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.DoubleArrayList.getValue(),
        ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(doubles));
  }

  @Override
  public DoubleArrayList deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE_ARRAY;
  }

  @Nullable
  @Override
  public DoubleArrayList extractFinalResult(@Nullable DoubleArrayList doubleArrayList) {
    if (doubleArrayList == null) {
      // A null intermediate result means nothing was aggregated. With null handling enabled the histogram of nothing
      // is NULL; with it disabled it is the all-zero histogram, which is the answer this mode has always given.
      return _nullHandlingEnabled ? null : DoubleVectorOpUtils.createAndInitialize(getNumBins());
    }
    return doubleArrayList;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSV(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateMV(length, aggregationResultHolder, blockValSet);
    }
  }

  private void aggregateSV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    double[] histogram = new double[getNumBins()];
    int numRows;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            increment(histogram, values[i]);
          }
          return acum + to - from;
        });
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            increment(histogram, values[i]);
          }
          return acum + to - from;
        });
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            increment(histogram, values[i]);
          }
          return acum + to - from;
        });
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            increment(histogram, values[i]);
          }
          return acum + to - from;
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            increment(histogram, values[i].doubleValue());
          }
          return acum + to - from;
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
    }
    // The histogram is published only when a row reached it, so a block with no non-null row leaves the holder
    // untouched and extractFinalResult sees the null that means nothing was aggregated. It is published once rather
    // than per range, because the buffer accumulates across ranges and adding it again would recount earlier rows.
    if (numRows > 0) {
      setAggregationResult(aggregationResultHolder, histogram);
    }
  }

  private void aggregateMV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    double[] histogram = new double[getNumBins()];
    int numRows;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[][] values = blockValSet.getIntValuesMV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (int value : values[i]) {
              increment(histogram, value);
            }
          }
          return acum + to - from;
        });
        break;
      }
      case LONG: {
        long[][] values = blockValSet.getLongValuesMV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (long value : values[i]) {
              increment(histogram, value);
            }
          }
          return acum + to - from;
        });
        break;
      }
      case FLOAT: {
        float[][] values = blockValSet.getFloatValuesMV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (float value : values[i]) {
              increment(histogram, value);
            }
          }
          return acum + to - from;
        });
        break;
      }
      case DOUBLE: {
        double[][] values = blockValSet.getDoubleValuesMV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (double value : values[i]) {
              increment(histogram, value);
            }
          }
          return acum + to - from;
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[][] values = blockValSet.getBigDecimalValuesMV();
        numRows = foldNotNull(length, blockValSet, 0, (acum, from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (BigDecimal value : values[i]) {
              increment(histogram, value.doubleValue());
            }
          }
          return acum + to - from;
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
    }
    if (numRows > 0) {
      setAggregationResult(aggregationResultHolder, histogram);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
    } else {
      aggregateMVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
    }
  }

  private void aggregateSVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            int value = values[i];
            setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
          }
        });
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            long value = values[i];
            setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
          }
        });
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            float value = values[i];
            setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
          }
        });
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            double value = values[i];
            setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            double value = values[i].doubleValue();
            setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
    }
  }

  private void aggregateMVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[][] values = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (int value : values[i]) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case LONG: {
        long[][] values = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (long value : values[i]) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case FLOAT: {
        float[][] values = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (float value : values[i]) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case DOUBLE: {
        double[][] values = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (double value : values[i]) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[][] values = blockValSet.getBigDecimalValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (BigDecimal value : values[i]) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, value.doubleValue());
            }
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
    } else {
      aggregateMVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
    }
  }

  private void aggregateSVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            int value = values[i];
            for (int groupKey : groupKeysArray[i]) {
              setGroupByResult(groupKey, groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            long value = values[i];
            for (int groupKey : groupKeysArray[i]) {
              setGroupByResult(groupKey, groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            float value = values[i];
            for (int groupKey : groupKeysArray[i]) {
              setGroupByResult(groupKey, groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            double value = values[i];
            for (int groupKey : groupKeysArray[i]) {
              setGroupByResult(groupKey, groupByResultHolder, value);
            }
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            double value = values[i].doubleValue();
            for (int groupKey : groupKeysArray[i]) {
              setGroupByResult(groupKey, groupByResultHolder, value);
            }
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
    }
  }

  private void aggregateMVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[][] values = blockValSet.getIntValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (int value : values[i]) {
              for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, value);
              }
            }
          }
        });
        break;
      }
      case LONG: {
        long[][] values = blockValSet.getLongValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (long value : values[i]) {
              for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, value);
              }
            }
          }
        });
        break;
      }
      case FLOAT: {
        float[][] values = blockValSet.getFloatValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (float value : values[i]) {
              for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, value);
              }
            }
          }
        });
        break;
      }
      case DOUBLE: {
        double[][] values = blockValSet.getDoubleValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (double value : values[i]) {
              for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, value);
              }
            }
          }
        });
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[][] values = blockValSet.getBigDecimalValuesMV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to && i < values.length; i++) {
            for (BigDecimal value : values[i]) {
              for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, value.doubleValue());
              }
            }
          }
        });
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute histogram for non-numeric type: "
            + blockValSet.getValueType());
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

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double[] histogram) {
    DoubleArrayList aggregatedHistogram = aggregationResultHolder.getResult();
    if (aggregatedHistogram == null) {
      aggregationResultHolder.setValue(DoubleVectorOpUtils.createAndInitialize(histogram));
    } else {
      DoubleVectorOpUtils.vectorAdd(aggregatedHistogram, histogram);
    }
  }
}
