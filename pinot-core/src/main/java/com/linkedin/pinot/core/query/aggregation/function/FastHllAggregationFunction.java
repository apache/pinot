/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import java.io.Serializable;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to implement the old 'fasthll' aggregation function,
 * only combine and reduce related methods are used,
 * calling other methods will throw UnsupportedOperationException.
 *
 * fasthll takes advantage of pre-aggregated results for fast distinct count estimation
 *
 * TODO: fully deprecate and remove this class when new aggregation routine is finished.
 */
public class FastHllAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastHllAggregationFunction.class);

  private String _distinctCountHLLColumnName;

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _distinctCountHLLColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public HyperLogLog aggregate(Block docIdSetBlock, Block[] block) {
    throw new UnsupportedOperationException("Calling old aggregate method of FastHllAggregationFunction is deprecated.");
  }

  @Override
  public HyperLogLog aggregate(HyperLogLog mergedResult, int docId, Block[] block) {
    throw new UnsupportedOperationException("Calling old aggregate method of FastHllAggregationFunction is deprecated.");
  }

  /**
   * shared with both old and new aggregation function
   * @param aggregationResultList
   * @param combineLevel
   * @return
   */
  @Override
  public List<HyperLogLog> combine(List<HyperLogLog> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }
    HyperLogLog merged = HllUtil.mergeHLLResultsToFirstInList(aggregationResultList);
    aggregationResultList.clear();
    aggregationResultList.add(merged);
    return aggregationResultList;
  }

  /**
   * shared with both old and new aggregation function
   * @param aggregationResult0
   * @param aggregationResult1
   * @return
   */
  @Override
  public HyperLogLog combineTwoValues(HyperLogLog aggregationResult0, HyperLogLog aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    try {
      aggregationResult0.addAll(aggregationResult1);
    } catch (CardinalityMergeException e) {
      LOGGER.error("Caught exception while merging Cardinality using HyperLogLog", e);
      Utils.rethrowException(e);
    }
    return aggregationResult0;
  }

  /**
   * shared with both old and new aggregation function
   * @param combinedResultList
   * @return
   */
  @Override
  public Long reduce(List<HyperLogLog> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0L;
    }
    HyperLogLog reducedResult = HllUtil.mergeHLLResultsToFirstInList(combinedResultList);
    return reducedResult.cardinality();
  }

  @Override
  public JSONObject render(Long finalAggregationResult) {
    try {
      return new JSONObject().put("value", finalAggregationResult.toString());
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering aggregation result", e);
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
    return "fasthll_" + _distinctCountHLLColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    throw new UnsupportedOperationException("Calling old getDefaultValue method of FastHllAggregationFunction is deprecated.");
  }
}
