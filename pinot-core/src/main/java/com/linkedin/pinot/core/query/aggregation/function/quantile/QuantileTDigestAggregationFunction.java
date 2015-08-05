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
package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.quantile.tdigest.TDigest;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Quantile function implemented using TDigest estimation.
 *
 */
public class QuantileTDigestAggregationFunction implements AggregationFunction<TDigest, Double> {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuantileTDigestAggregationFunction.class);
  public static final int DEFAULT_COMPRESSION_FACTOR = 100;

  private String _tDigestColumnName;
  private int _compressionFactor;
  private byte _quantile; // 0-100

  public QuantileTDigestAggregationFunction(byte quantile) {
    this(quantile, DEFAULT_COMPRESSION_FACTOR);
  }

  public QuantileTDigestAggregationFunction(byte quantile, int compressionFactor) {
    _quantile = quantile;
    _compressionFactor = compressionFactor;
  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _tDigestColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  private void offerValueToTDigest(int docId, Block[] block, TDigest digest) {
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    if (blockValIterator.skipTo(docId)) {
      int dictionaryIndex = blockValIterator.nextIntVal();
      if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
        digest.offer(((Number) dictionaryReader.get(dictionaryIndex)).doubleValue());
      } else {
        // ignore this
        LOGGER.info("ignore NULL_VALUE_INDEX");
      }
    }
  }

  @Override
  public TDigest aggregate(Block docIdSetBlock, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (!type.isNumber()) {
      throw new RuntimeException("Only number column can be used in quantile, get: " + type);
    }

    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    TDigest ret = new TDigest(_compressionFactor);
    int docId = 0;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      offerValueToTDigest(docId, block, ret);
    }

    return ret;
  }

  @Override
  public TDigest aggregate(TDigest mergedResult, int docId, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (!type.isNumber()) {
      throw new RuntimeException("Only number column can be used in quantile, get: " + type);
    }

    if (mergedResult == null) {
      mergedResult = new TDigest(_compressionFactor);
    }
    offerValueToTDigest(docId, block, mergedResult);
    return mergedResult;
  }

  @Override
  public List<TDigest> combine(List<TDigest> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }

    TDigest tDigestResult = TDigest.merge(aggregationResultList);
    aggregationResultList.clear();
    aggregationResultList.add(tDigestResult);
    return aggregationResultList;
  }

  @Override
  public TDigest combineTwoValues(TDigest aggregationResult0, TDigest aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }

    return TDigest.merge(aggregationResult0, aggregationResult1);
  }

  @Override
  public Double reduce(List<TDigest> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0.0;
    }

    TDigest merged = TDigest.merge(combinedResultList);
    Double ret = merged.getQuantile(_quantile);
    return ret;
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
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
    return "quantile" + _quantile + "_" + _tDigestColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new TDigest(_compressionFactor);
  }
}
