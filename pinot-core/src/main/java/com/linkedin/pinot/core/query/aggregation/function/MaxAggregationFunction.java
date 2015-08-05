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
import com.linkedin.pinot.common.Utils;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaxAggregationFunction implements AggregationFunction<Double, Double> {
  private static final double DEFAULT_VALUE = Double.NEGATIVE_INFINITY;
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxAggregationFunction.class);

  private String _maxColumnName;

  public MaxAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _maxColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] block) {
    double ret = DEFAULT_VALUE;
    double tmp = 0;
    int docId = 0;
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        int dictionaryIndex = blockValIterator.nextIntVal();
        if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
          tmp = dictionaryReader.getDoubleValue(dictionaryIndex);
          if (tmp > ret) {
            ret = tmp;
          }
        }
      }
    }
    return ret;
  }

  @Override
  public Double aggregate(Double mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      int dictionaryIndex = blockValIterator.nextIntVal();
      if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
        double value = block[0].getMetadata().getDictionary().getDoubleValue(dictionaryIndex);
        if (mergedResult == null) {
          return value;
        } else {
          return Math.max(value, mergedResult);
        }
      } else {
        return mergedResult;
      }
    }
    return mergedResult;
  }

  @Override
  public List<Double> combine(List<Double> aggregationResultList, CombineLevel combineLevel) {
    double maxValue = DEFAULT_VALUE;
    for (double aggregationResult : aggregationResultList) {
      if (maxValue < aggregationResult) {
        maxValue = aggregationResult;
      }
    }
    aggregationResultList.clear();
    aggregationResultList.add(maxValue);
    return aggregationResultList;
  }

  @Override
  public Double combineTwoValues(Double aggregationResult0, Double aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return (aggregationResult0 > aggregationResult1) ? aggregationResult0 : aggregationResult1;
  }

  @Override
  public Double reduce(List<Double> combinedResultList) {
    double maxValue = DEFAULT_VALUE;
    for (double combinedResult : combinedResultList) {
      if (maxValue < combinedResult) {
        maxValue = combinedResult;
      }
    }
    return maxValue;
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        throw new NoSuchElementException("Final result is null!");
      }
      if (finalAggregationResult.isInfinite()) {
        return new JSONObject().put("value", "null");
      }
      return new JSONObject().put("value", String.format(Locale.US, "%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering as JSON", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.DOUBLE;
  }

  @Override
  public String getFunctionName() {
    return "max_" + _maxColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new Double(DEFAULT_VALUE);
  }

}
