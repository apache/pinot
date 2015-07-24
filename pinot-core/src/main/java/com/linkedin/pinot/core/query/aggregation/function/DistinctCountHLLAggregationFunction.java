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

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

/**
 * Distinct Count implemented using HyperLogLog estimation.
 *
 * The AggregateResult Type is HyperLogLog (registerSet)
 * The ReduceResult Type is Integer (estimation)
 *
 * HyperLogLog:
 * It is the state-of-art method for distinct count estimation, according to paper
 * @see <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf">
 *     HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm</a>
 *
 * "Let delta = 1.04/sqrt(m) represents the standard error; the estimates provided by HYPERLOGLOG
 * are expected to be within delta, 2delta, 3delta of the exact count in respectively 65%, 95%, 99% of all
 * the cases." Thus, for the m = 1024 case, the error within 3.25%, 6.5% and 9.75% are in the confidence
 * of 65%, 95% and 99% respectively.
 *
 * Warning:
 * 1. _bitSize, i.e. log of bucket size m (m=2^_bitSize), significantly affect the merge speed, size between 8 to 13 is common choice.
 *    see {@link RegisterSet#merge(RegisterSet)}  }
 * 2. This implementation uses HyperLogLog provided hash function, i.e. {@link MurmurHash}, other hash functions may not work.
 *    see {@link HyperLogLog#offer(Object)}
 * 3. HyperLogLog directly used as the AggregateResult Type since a wrapper class may affect the speed (inheritance is ok)
 *
 * author: Jiaqi Gu
 *
 */
public class DistinctCountHLLAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCountHLLAggregationFunction.class);
  public static final int DEFAULT_BIT_SIZE = 10;

  private String _distinctCountHLLColumnName;
  private int _bitSize;

  public DistinctCountHLLAggregationFunction() {
    _bitSize = DEFAULT_BIT_SIZE;
  }

  public DistinctCountHLLAggregationFunction(int bitSize) {
    _bitSize = bitSize;
  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _distinctCountHLLColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public HyperLogLog aggregate(Block docIdSetBlock, Block[] block) {
    HyperLogLog ret = new HyperLogLog(_bitSize);
    int docId = 0;
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    if (block[0].getMetadata().getDataType() == DataType.STRING) {
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        if (blockValIterator.skipTo(docId)) {
          int dictionaryIndex = blockValIterator.nextIntVal();
          if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
            // use hash function defined in Hyperloglog, default may not work
            ret.offer(dictionaryReader.get(dictionaryIndex));
          } else {
            ret.offer(Integer.MIN_VALUE);
          }
        }
      }
    } else {
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        if (blockValIterator.skipTo(docId)) {
          int dictionaryIndex = blockValIterator.nextIntVal();
          if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
            // use hash function defined in Hyperloglog, default may not work
            ret.offer(((Number) dictionaryReader.get(dictionaryIndex)));
          } else {
            ret.offer(Integer.MIN_VALUE);
          }
        }
      }
    }
    return ret;
  }

  @Override
  public HyperLogLog aggregate(HyperLogLog mergedResult, int docId, Block[] block) {
    if (mergedResult == null) {
      mergedResult = new HyperLogLog(_bitSize);
    }
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      int dictId = blockValIterator.nextIntVal();
      if (dictId != Dictionary.NULL_VALUE_INDEX) {
        if (block[0].getMetadata().getDataType() == DataType.STRING) {
          mergedResult.offer(block[0].getMetadata().getDictionary().get(dictId));
        } else {
          mergedResult.offer(((Number) block[0].getMetadata().getDictionary().get(dictId)));
        }
      } else {
        mergedResult.offer(Integer.MIN_VALUE);
      }
    }
    return mergedResult;
  }

  @Override
  public List<HyperLogLog> combine(List<HyperLogLog> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }
    HyperLogLog hllResult = aggregationResultList.get(0);
    for (int i = 1; i < aggregationResultList.size(); ++i) {
      try {
        hllResult.addAll(aggregationResultList.get(i));
      } catch (CardinalityMergeException e) {
        e.printStackTrace();
      }
    }
    aggregationResultList.clear();
    aggregationResultList.add(hllResult);
    return aggregationResultList;
  }

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
      e.printStackTrace();
    }
    return aggregationResult0;
  }

  @Override
  public Long reduce(List<HyperLogLog> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0L;
    }
    HyperLogLog reducedResult = combinedResultList.get(0);
    for (int i = 1; i < combinedResultList.size(); ++i) {
      try {
        reducedResult.addAll(combinedResultList.get(i));
      } catch (CardinalityMergeException e) {
        e.printStackTrace();
      }
    }
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
    return "distinctCountHLL_" + _distinctCountHLLColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new HyperLogLog(_bitSize);
  }
}
