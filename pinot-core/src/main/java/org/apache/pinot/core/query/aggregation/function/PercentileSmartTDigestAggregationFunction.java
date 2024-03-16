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
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleListIterator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
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
 * The {@code PercentileSmartTDigestAggregationFunction} calculates the percentile of the values for a given expression
 * (both single-valued and multi-valued are supported).
 *
 * For aggregation-only queries, the values are stored in a {@link DoubleArrayList} initially. Once the number of values
 * exceeds a threshold, the list will be converted into a {@link TDigest}, and approximate result will be returned.
 *
 * The function takes an optional third argument for parameters:
 * - threshold: Threshold of the number of values to trigger the conversion, 100_000 by default. Non-positive value
 *              means never convert.
 * - compression: Compression for the converted TDigest, 100 by default.
 * Example of third argument: 'threshold=10000;compression=50'
 */
public class PercentileSmartTDigestAggregationFunction extends NullableSingleInputAggregationFunction<Object, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  private final double _percentile;
  private final int _threshold;
  private final int _compression;

  public PercentileSmartTDigestAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    try {
      _percentile = arguments.get(1).getLiteral().getDoubleValue();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Second argument of PERCENTILE_SMART_TDIGEST aggregation function must be a double literal (percentile)");
    }
    Preconditions.checkArgument(_percentile >= 0 && _percentile <= 100, "Invalid percentile: %s", _percentile);
    if (arguments.size() > 2) {
      Parameters parameters = new Parameters(arguments.get(2).getLiteral().getStringValue());
      _compression = parameters._compression;
      _threshold = parameters._threshold;
    } else {
      _threshold = Parameters.DEFAULT_THRESHOLD;
      _compression = Parameters.DEFAULT_COMPRESSION;
    }
  }

  public double getPercentile() {
    return _percentile;
  }

  public int getThreshold() {
    return _threshold;
  }

  public int getCompression() {
    return _compression;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILESMARTTDIGEST;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.PERCENTILESMARTTDIGEST.getName().toLowerCase() + "(" + _expression + ", "
        + _percentile + ")";
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
    validateValueType(blockValSet);
    if (aggregationResultHolder.getResult() instanceof TDigest) {
      aggregateIntoTDigest(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateIntoValueList(length, aggregationResultHolder, blockValSet);
    }
  }

  private static void validateValueType(BlockValSet blockValSet) {
    DataType valueType = blockValSet.getValueType();
    Preconditions.checkArgument(valueType.getStoredType().isNumeric(),
        "Illegal data type for PERCENTILE_SMART_TDIGEST aggregation function: %s%s", valueType,
        blockValSet.isSingleValue() ? "" : "_MV");
  }

  private void aggregateIntoTDigest(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (blockValSet.isSingleValue()) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          tDigest.add(doubleValues[i]);
        }
      });
    } else {
      double[][] doubleValues = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (double value : doubleValues[i]) {
            tDigest.add(value);
          }
        }
      });
    }
  }

  private DoubleArrayList getOrCreateList(int length, AggregationResultHolder aggregationResultHolder) {
    DoubleArrayList valueList = aggregationResultHolder.getResult();
    if (valueList == null) {
      valueList = new DoubleArrayList(length);
      aggregationResultHolder.setValue(valueList);
    }
    return valueList;
  }

  private void aggregateIntoValueList(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet) {
    DoubleArrayList valueList = getOrCreateList(length, aggregationResultHolder);
    if (blockValSet.isSingleValue()) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, toEx) ->
        valueList.addElements(valueList.size(), doubleValues, from, toEx - from)
      );
    } else {
      double[][] doubleValues = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, toEx) ->
        valueList.addElements(valueList.size(), doubleValues[from], 0, toEx - from)
      );
    }
    if (valueList.size() > _threshold) {
      aggregationResultHolder.setValue(convertValueListToTDigest(valueList));
    }
  }

  private TDigest convertValueListToTDigest(DoubleArrayList valueList) {
    TDigest tDigest = TDigest.createMergingDigest(_compression);
    DoubleListIterator iterator = valueList.iterator();
    while (iterator.hasNext()) {
      tDigest.add(iterator.nextDouble());
    }
    return tDigest;
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    validateValueType(blockValSet);
    if (blockValSet.isSingleValue()) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          DoubleArrayList valueList = getValueList(groupByResultHolder, groupKeyArray[i]);
          valueList.add(doubleValues[i]);
        }
      });
    } else {
      double[][] doubleValues = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          DoubleArrayList valueList = getValueList(groupByResultHolder, groupKeyArray[i]);
          valueList.addElements(valueList.size(), doubleValues[i]);
        }
      });
    }
  }

  private static DoubleArrayList getValueList(GroupByResultHolder groupByResultHolder, int groupKey) {
    DoubleArrayList valueList = groupByResultHolder.getResult(groupKey);
    if (valueList == null) {
      valueList = new DoubleArrayList();
      groupByResultHolder.setValueForKey(groupKey, valueList);
    }
    return valueList;
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    validateValueType(blockValSet);
    if (blockValSet.isSingleValue()) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getValueList(groupByResultHolder, groupKey).add(doubleValues[i]);
          }
        }
      });
    } else {
      double[][] doubleValues = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            DoubleArrayList valueList = getValueList(groupByResultHolder, groupKey);
            valueList.addElements(valueList.size(), doubleValues[i]);
          }
        }
      });
    }
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    return result != null ? result : new DoubleArrayList();
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    return result != null ? result : new DoubleArrayList();
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    if (intermediateResult1 instanceof TDigest) {
      return mergeIntoTDigest((TDigest) intermediateResult1, intermediateResult2);
    }
    if (intermediateResult2 instanceof TDigest) {
      return mergeIntoTDigest((TDigest) intermediateResult2, intermediateResult1);
    }
    DoubleArrayList valueList1 = (DoubleArrayList) intermediateResult1;
    DoubleArrayList valueList2 = (DoubleArrayList) intermediateResult2;
    valueList1.addAll(valueList2);
    return valueList1.size() > _threshold ? convertValueListToTDigest(valueList1) : valueList1;
  }

  private static TDigest mergeIntoTDigest(TDigest tDigest, Object intermediateResult) {
    if (intermediateResult instanceof TDigest) {
      tDigest.add((TDigest) intermediateResult);
    } else {
      DoubleArrayList valueList = (DoubleArrayList) intermediateResult;
      DoubleListIterator iterator = valueList.iterator();
      while (iterator.hasNext()) {
        tDigest.add(iterator.nextDouble());
      }
    }
    return tDigest;
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
  public Double extractFinalResult(Object intermediateResult) {
    if (intermediateResult instanceof TDigest) {
      return ((TDigest) intermediateResult).quantile(_percentile / 100.0);
    } else {
      DoubleArrayList valueList = (DoubleArrayList) intermediateResult;
      int size = valueList.size();
      if (size == 0) {
        if (_nullHandlingEnabled) {
          return null;
        } else {
          return DEFAULT_FINAL_RESULT;
        }
      } else {
        double[] values = valueList.elements();
        Arrays.sort(values, 0, size);
        if (_percentile == 100) {
          return values[size - 1];
        } else {
          return values[(int) ((long) size * _percentile / 100)];
        }
      }
    }
  }

  /**
   * Helper class to wrap the parameters.
   */
  private static class Parameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String THRESHOLD_KEY = "THRESHOLD";
    static final int DEFAULT_THRESHOLD = 100_000;
    static final String COMPRESSION_KEY = "COMPRESSION";
    static final int DEFAULT_COMPRESSION = 100;

    int _threshold = DEFAULT_THRESHOLD;
    int _compression = DEFAULT_COMPRESSION;

    Parameters(String parametersString) {
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        switch (key.toUpperCase()) {
          case THRESHOLD_KEY:
            _threshold = Integer.parseInt(value);
            // Treat non-positive threshold as unlimited
            if (_threshold <= 0) {
              _threshold = Integer.MAX_VALUE;
            }
            break;
          case COMPRESSION_KEY:
            _compression = Integer.parseInt(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }
  }
}
