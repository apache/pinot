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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;



public class ScanBasedSingleValueDocIdSet implements FilterBlockDocIdSet {
  private final BlockValSet blockValSet;
  private BlockValSetBasedDocIdIterator blockValSetBlockDocIdIterator;
  private BlockMetadata blockMetadata;

  public ScanBasedSingleValueDocIdSet(BlockValSet blockValSet, BlockMetadata blockMetadata, PredicateEvaluator evaluator) {
    this.blockValSet = blockValSet;
    this.blockMetadata = blockMetadata;
    blockValSetBlockDocIdIterator = new BlockValSetBasedDocIdIterator(blockValSet, blockMetadata, evaluator);
  }

  public int getMinDocId() {
    return blockMetadata.getStartDocId();
  }

  public int getMaxDocId() {
    return blockMetadata.getEndDocId();
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  public void setStartDocId(int startDocId) {
    blockValSetBlockDocIdIterator.setStartDocId(startDocId);
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  public void setEndDocId(int endDocId) {
    blockValSetBlockDocIdIterator.setEndDocId(endDocId);
  }

  @Override
  public BlockDocIdIterator iterator() {
    return blockValSetBlockDocIdIterator;
  }

  public static class BlockValSetBasedDocIdIterator implements BlockDocIdIterator {
    int currentDocId = -1;
    BlockSingleValIterator valueIterator;
    private int startDocId;
    private int endDocId;
    private PredicateEvaluator evaluator;

    public BlockValSetBasedDocIdIterator(BlockValSet blockValSet, BlockMetadata blockMetadata, PredicateEvaluator evaluator) {
      this.evaluator = evaluator;
      if (evaluator.getMatchingDictionaryIds().length == 0) {
        currentDocId = Constants.EOF;
        setStartDocId(Constants.EOF);
        setEndDocId(Constants.EOF);
      } else {
        setStartDocId(blockMetadata.getStartDocId());
        setEndDocId(blockMetadata.getEndDocId());
      }
      valueIterator = (BlockSingleValIterator) blockValSet.iterator();
    }

    /**
     * After setting the startDocId, next calls will always return from &gt;=startDocId
     * @param startDocId
     */
    public void setStartDocId(int startDocId) {
      this.startDocId = startDocId;
    }

    /**
     * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
     * @param endDocId
     */
    public void setEndDocId(int endDocId) {
      this.endDocId = endDocId;
    }

    @Override
    public int advance(int targetDocId) {
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      if (targetDocId < startDocId) {
        targetDocId = startDocId;
      } else if (targetDocId > endDocId) {
        currentDocId = Constants.EOF;
      }
      if (currentDocId >= targetDocId) {
        return currentDocId;
      } else {
        currentDocId = targetDocId - 1;
        valueIterator.skipTo(targetDocId);
        return next();
      }
    }

    @Override
    public int next() {
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      while (valueIterator.hasNext() && currentDocId <= endDocId) {
        currentDocId = currentDocId + 1;
        int dictIdForCurrentDoc = valueIterator.nextIntVal();
        if (evaluator.apply(dictIdForCurrentDoc)) {
          //System.out.println("Returning deom " + this +"doc Id:"+ currentDocId  + " dictId:"+ dictIdForCurrentDoc);
          return currentDocId;
        }
      }
      currentDocId = Constants.EOF;
      return Constants.EOF;
    }

    @Override
    public int currentDocId() {
      return currentDocId;
    }
  };

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
  }

}
