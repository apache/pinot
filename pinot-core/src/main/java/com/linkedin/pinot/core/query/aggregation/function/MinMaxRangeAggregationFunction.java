/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction.MinMaxRangePair;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

/**
 * This function will take a column and do min/max and compute diff on that.
 */
public class MinMaxRangeAggregationFunction implements AggregationFunction<MinMaxRangePair, Double> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinMaxRangeAggregationFunction.class);
  private static final double DEFAULT_MIN_MAX_RANGE_VALUE = -1;

  private String _minMaxRangeByColumn;

  public MinMaxRangeAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _minMaxRangeByColumn = aggregationInfo.getAggregationParams().get("column");

  }

  @Override
  public MinMaxRangePair aggregate(Block docIdSetBlock, Block[] block) {
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    int docId = 0;
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator =
        (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        int dictionaryIndex = blockValIterator.nextIntVal();
        if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
          min = Math.min(min, dictionaryReader.getDoubleValue(dictionaryIndex));
          max = Math.max(max, dictionaryReader.getDoubleValue(dictionaryIndex));
        }
      }
    }
    return new MinMaxRangePair(min, max);
  }

  @Override
  public MinMaxRangePair aggregate(MinMaxRangePair mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator =
        (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      int dictId = blockValIterator.nextIntVal();
      if (dictId != Dictionary.NULL_VALUE_INDEX) {
        double currentValue = block[0].getMetadata().getDictionary().getDoubleValue(dictId);
        if (mergedResult == null) {
          return new MinMaxRangePair(currentValue, currentValue);
        }
        return new MinMaxRangePair(Math.min(mergedResult.getFirst(), currentValue),
            Math.max(mergedResult.getSecond(), currentValue));
      }
    }
    return mergedResult;
  }

  @Override
  public List<MinMaxRangePair> combine(List<MinMaxRangePair> aggregationResultList, CombineLevel combineLevel) {
    double combinedMinResult = Double.POSITIVE_INFINITY;
    double combinedMaxResult = Double.NEGATIVE_INFINITY;
    for (MinMaxRangePair aggregationResult : aggregationResultList) {
      combinedMinResult = Math.min(combinedMinResult, aggregationResult.getFirst());
      combinedMaxResult = Math.max(combinedMaxResult, aggregationResult.getSecond());
    }
    aggregationResultList.clear();
    aggregationResultList.add(new MinMaxRangePair(combinedMinResult, combinedMaxResult));
    return aggregationResultList;
  }

  @Override
  public MinMaxRangePair combineTwoValues(MinMaxRangePair aggregationResult0, MinMaxRangePair aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return new MinMaxRangePair(Math.min(aggregationResult0.getFirst(), aggregationResult1.getFirst()),
        Math.max(aggregationResult0.getSecond(), aggregationResult1.getSecond()));
  }

  @Override
  public Double reduce(List<MinMaxRangePair> combinedResult) {
    if (combinedResult.isEmpty()) {
      return DEFAULT_MIN_MAX_RANGE_VALUE;
    }

    double reducedMinResult = Double.POSITIVE_INFINITY;
    double reducedMaxResult = Double.NEGATIVE_INFINITY;
    for (MinMaxRangePair combineResult : combinedResult) {
      reducedMinResult = Math.min(reducedMinResult, combineResult.getFirst());
      reducedMaxResult = Math.max(reducedMaxResult, combineResult.getSecond());
    }
    if (reducedMinResult != Double.POSITIVE_INFINITY
        && reducedMaxResult != Double.NEGATIVE_INFINITY) {
      return reducedMaxResult - reducedMinResult;
    } else {
      return DEFAULT_MIN_MAX_RANGE_VALUE;
    }
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if ((finalAggregationResult == null) || (Double.isNaN(finalAggregationResult))) {
        return new JSONObject().put("value", DEFAULT_MIN_MAX_RANGE_VALUE);
      }
      return new JSONObject().put("value",
          String.format(Locale.US, "%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering to JSON", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.OBJECT;
  }

  @Override
  public String getFunctionName() {
    return "minMaxRange_" + _minMaxRangeByColumn;
  }

  public static class MinMaxRangePair extends Pair<Double, Double>
      implements Comparable<MinMaxRangePair>, Serializable {
    private static final long serialVersionUID = 498953067962085988L;
    public MinMaxRangePair(Double first, Double second) {
      super(first, second);
    }

    @Override
    public int compareTo(MinMaxRangePair o) {
      double diff = getSecond() - getFirst() - o.getSecond() + o.getFirst();
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      }
      return 0;
    }

    @Override
    public String toString() {
      return new DecimalFormat("####################.##########",
          DecimalFormatSymbols.getInstance(Locale.US)).format((getSecond() - getFirst()));
    }
  }

  public MinMaxRangePair getMinMaxRangePair(double first, double second) {
    return new MinMaxRangePair(first, second);
  }

  @Override
  public Serializable getDefaultValue() {
    return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
  }
}
