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
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * This function will take a column and do sum on that.
 *
 */
public class AvgAggregationFunction implements AggregationFunction<AvgPair, Double> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvgAggregationFunction.class);

  private String _avgByColumn;

  public AvgAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _avgByColumn = aggregationInfo.getAggregationParams().get("column");

  }

  @Override
  public AvgPair aggregate(Block docIdSetBlock, Block[] block) {
    double ret = 0;
    long cnt = 0;
    int docId = 0;
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        int dictionaryIndex = blockValIterator.nextIntVal();
        if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
          ret += dictionaryReader.getDoubleValue(dictionaryIndex);
          cnt++;
        }
      }
    }
    return new AvgPair(ret, cnt);
  }

  @Override
  public AvgPair aggregate(AvgPair mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      int dictId = blockValIterator.nextIntVal();
      if (dictId != Dictionary.NULL_VALUE_INDEX) {
        if (mergedResult == null) {
          return new AvgPair(block[0].getMetadata().getDictionary().getDoubleValue(dictId), (long) 1);
        }
        return new AvgPair(mergedResult.getFirst() + block[0].getMetadata().getDictionary().getDoubleValue(dictId),
            mergedResult.getSecond() + 1);
      }
    }
    return mergedResult;
  }

  @Override
  public List<AvgPair> combine(List<AvgPair> aggregationResultList, CombineLevel combineLevel) {
    double combinedSumResult = 0;
    long combinedCntResult = 0;
    for (AvgPair aggregationResult : aggregationResultList) {
      combinedSumResult += aggregationResult.getFirst();
      combinedCntResult += aggregationResult.getSecond();
    }
    aggregationResultList.clear();
    aggregationResultList.add(new AvgPair(combinedSumResult, combinedCntResult));
    return aggregationResultList;
  }

  @Override
  public AvgPair combineTwoValues(AvgPair aggregationResult0, AvgPair aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return new AvgPair(aggregationResult0.getFirst() + aggregationResult1.getFirst(), aggregationResult0.getSecond()
        + aggregationResult1.getSecond());
  }

  @Override
  public Double reduce(List<AvgPair> combinedResult) {

    double reducedSumResult = 0;
    long reducedCntResult = 0;
    for (AvgPair combineResult : combinedResult) {
      reducedSumResult += combineResult.getFirst();
      reducedCntResult += combineResult.getSecond();
    }
    if (reducedCntResult > 0) {
      double avgResult = reducedSumResult / reducedCntResult;
      return avgResult;
    } else {
      return Double.NaN;
    }
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if ((finalAggregationResult == null) || (Double.isNaN(finalAggregationResult))) {
        return new JSONObject().put("value", Dictionary.NULL_VALUE_INDEX);
      }
      return new JSONObject().put("value", String.format(Locale.US, "%1.5f", finalAggregationResult));
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
    return "avg_" + _avgByColumn;
  }

  public class AvgPair extends Pair<Double, Long> implements Comparable<AvgPair>, Serializable {
    public AvgPair(Double first, Long second) {
      super(first, second);
    }

    @Override
    public int compareTo(AvgPair o) {
      if (getSecond() == 0) {
        return -1;
      }
      if (o.getSecond() == 0) {
        return 1;
      }
      if ((getFirst() / getSecond()) > (o.getFirst() / o.getSecond())) {
        return 1;
      }
      if ((getFirst() / getSecond()) < (o.getFirst() / o.getSecond())) {
        return -1;
      }
      return 0;
    }

    @Override
    public String toString() {
      if (getSecond() != 0) {
        return new DecimalFormat("####################.##########", DecimalFormatSymbols.getInstance(Locale.US)).format((getFirst() / getSecond()));
      } else {
        return "0.0";
      }
    }
  }

  public AvgPair getAvgPair(double first, long second) {
    return new AvgPair(first, second);
  }

  @Override
  public Serializable getDefaultValue() {
    return new AvgPair(0.0, 0L);
  }
}
