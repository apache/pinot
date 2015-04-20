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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.List;

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


public class DistinctCountAggregationFunction implements AggregationFunction<IntOpenHashSet, Integer> {

  private String _distinctCountColumnName;

  public DistinctCountAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _distinctCountColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public IntOpenHashSet aggregate(Block docIdSetBlock, Block[] block) {
    IntOpenHashSet ret = new IntOpenHashSet();
    int docId = 0;
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    if (block[0].getMetadata().getDataType() == DataType.STRING) {
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        if (blockValIterator.skipTo(docId)) {
          ret.add(dictionaryReader.get(blockValIterator.nextIntVal()).hashCode());
        }
      }
    } else {
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        if (blockValIterator.skipTo(docId)) {
          ret.add(((Number) dictionaryReader.get(blockValIterator.nextIntVal())).intValue());
        }
      }
    }
    return ret;
  }

  @Override
  public IntOpenHashSet aggregate(IntOpenHashSet mergedResult, int docId, Block[] block) {
    if (mergedResult == null) {
      mergedResult = new IntOpenHashSet();
    }
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      if (block[0].getMetadata().getDataType() == DataType.STRING) {
        mergedResult.add(block[0].getMetadata().getDictionary().get(blockValIterator.nextIntVal()).hashCode());
      } else {
        mergedResult.add(((Number) block[0].getMetadata().getDictionary().get(blockValIterator.nextIntVal())).intValue());
      }
    }
    return mergedResult;
  }

  @Override
  public List<IntOpenHashSet> combine(List<IntOpenHashSet> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }
    IntOpenHashSet intOpenHashSet = aggregationResultList.get(0);
    for (int i = 1; i < aggregationResultList.size(); ++i) {
      intOpenHashSet.addAll(aggregationResultList.get(i));
    }
    aggregationResultList.clear();
    aggregationResultList.add(intOpenHashSet);
    return aggregationResultList;
  }

  @Override
  public IntOpenHashSet combineTwoValues(IntOpenHashSet aggregationResult0, IntOpenHashSet aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    aggregationResult0.addAll(aggregationResult1);
    return aggregationResult0;
  }

  @Override
  public Integer reduce(List<IntOpenHashSet> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0;
    }
    IntOpenHashSet reducedResult = combinedResultList.get(0);
    for (int i = 1; i < combinedResultList.size(); ++i) {
      reducedResult.addAll(combinedResultList.get(i));
    }
    return reducedResult.size();
  }

  @Override
  public JSONObject render(Integer finalAggregationResult) {
    try {
      return new JSONObject().put("value", finalAggregationResult.toString());
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.OBJECT;
  }

  @Override
  public String getFunctionName() {
    return "distinctCount_" + _distinctCountColumnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new IntOpenHashSet();
  }

}
