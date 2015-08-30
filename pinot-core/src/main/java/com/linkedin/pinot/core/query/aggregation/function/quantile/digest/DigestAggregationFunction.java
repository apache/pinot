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
package com.linkedin.pinot.core.query.aggregation.function.quantile.digest;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Percentile(Quantile) function implemented using Digest estimation.
 *
 */
public class DigestAggregationFunction implements AggregationFunction<QuantileDigest, Long> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DigestAggregationFunction.class);
  public static final double DEFAULT_MAX_ERROR = 0.05;

  private String _digestColumnName;
  private double _maxError;
  private double _quantile; // 0.0-1.0

  public DigestAggregationFunction(byte percentile) {
    this(percentile, DEFAULT_MAX_ERROR);
  }

  public DigestAggregationFunction(byte percentile, double maxError) {
    _quantile = (percentile + 0.0) / 100;
    _maxError = maxError;
  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _digestColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  private void offerValueToTDigest(int docId, Block[] block, QuantileDigest digest) {
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    if (blockValIterator.skipTo(docId)) {
      int dictionaryIndex = blockValIterator.nextIntVal();
      if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
        digest.add(((Number) dictionaryReader.get(dictionaryIndex)).longValue());
      } else {
        // ignore this
        LOGGER.info("ignore NULL_VALUE_INDEX");
      }
    }
  }

  @Override
  public QuantileDigest aggregate(Block docIdSetBlock, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (!type.isInteger()) {
      throw new RuntimeException("Only integer(byte, short, int, long) type columns can be used in percentileest, get: " + type);
    }

    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    QuantileDigest ret = new QuantileDigest(_maxError);
    int docId = 0;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      offerValueToTDigest(docId, block, ret);
    }

    return ret;
  }

  @Override
  public QuantileDigest aggregate(QuantileDigest mergedResult, int docId, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (!type.isInteger()) {
      throw new RuntimeException("Only integer(byte, short, int, long) type columns can be used in percentileest, get: " + type);
    }

    if (mergedResult == null) {
      mergedResult = new QuantileDigest(_maxError);
    }
    offerValueToTDigest(docId, block, mergedResult);
    return mergedResult;
  }

  @Override
  public List<QuantileDigest> combine(List<QuantileDigest> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }

    QuantileDigest digestResult = QuantileDigest.merge(aggregationResultList);
    aggregationResultList.clear();
    aggregationResultList.add(digestResult);
    return aggregationResultList;
  }

  @Override
  public QuantileDigest combineTwoValues(QuantileDigest aggregationResult0, QuantileDigest aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }

    aggregationResult0.merge(aggregationResult1);
    return aggregationResult0;
  }

  @Override
  public Long reduce(List<QuantileDigest> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0L;
    }

    QuantileDigest merged = QuantileDigest.merge(combinedResultList);
    Long ret = merged.getQuantile(_quantile);
    return ret;
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
    return "percentileEst" + (int)(_quantile * 100) + "_" + _digestColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new QuantileDigest(DEFAULT_MAX_ERROR);
  }
}
