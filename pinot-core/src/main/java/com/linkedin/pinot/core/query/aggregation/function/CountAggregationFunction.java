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

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This function will take a column and do sum on that.
 *
 */
public class CountAggregationFunction implements AggregationFunction<Long, Long> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountAggregationFunction.class);

  public CountAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {

  }

  @Override
  public Long aggregate(Block docIdSetBlock, Block[] block) {
    return (long) ((DocIdSetBlock) docIdSetBlock).getSearchableLength();
  }

  @Override
  public Long aggregate(Long mergedResult, int docId, Block[] block) {
    if (mergedResult == null) {
      return (long) 1;
    } else {
      return (mergedResult + 1);
    }
  }

  @Override
  public List<Long> combine(List<Long> aggregationResultList, CombineLevel combineLevel) {
    long combinedValue = 0;
    for (Long value : aggregationResultList) {
      combinedValue += value;
    }
    aggregationResultList.clear();
    aggregationResultList.add(combinedValue);
    return aggregationResultList;
  }

  @Override
  public Long combineTwoValues(Long aggregationResult0, Long aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return aggregationResult0 + aggregationResult1;
  }

  @Override
  public Long reduce(List<Long> combinedResultList) {
    long reducedValue = 0;
    for (Long value : combinedResultList) {
      reducedValue += value;
    }
    return reducedValue;
  }

  @Override
  public JSONObject render(Long reduceResult) {
    try {
      if (reduceResult == null) {
        reduceResult = new Long(0);
      }
      return new JSONObject().put("value", reduceResult.toString());
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering to JSON", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.LONG;
  }

  @Override
  public String getFunctionName() {
    return "count_star";
  }

  @Override
  public Serializable getDefaultValue() {
    return Long.valueOf(0);
  }

}
