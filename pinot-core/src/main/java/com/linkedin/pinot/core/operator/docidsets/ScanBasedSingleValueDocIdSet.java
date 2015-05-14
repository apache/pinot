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

import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;


public class ScanBasedSingleValueDocIdSet implements FilterBlockDocIdSet {
  private final BlockValSet blockValSet;
  private BlockValSetBasedDocIdIterator blockValSetBlockDocIdIterator;
  private BlockMetadata blockMetadata;

  public ScanBasedSingleValueDocIdSet(BlockValSet blockValSet, BlockMetadata blockMetadata, int... dictIds) {
    this.blockValSet = blockValSet;
    this.blockMetadata = blockMetadata;
    blockValSetBlockDocIdIterator = new BlockValSetBasedDocIdIterator(blockValSet, blockMetadata, dictIds);
  }

  public int getMinDocId() {
    return blockMetadata.getStartDocId();
  }

  public int getMaxDocId() {
    return blockMetadata.getEndDocId();
  }

  /**
   * After setting the startDocId, next calls will always return from >=startDocId
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
    private Set<Integer> dictIdSet;
    private int startDocId;
    private int endDocId;

    public BlockValSetBasedDocIdIterator(BlockValSet blockValSet, BlockMetadata blockMetadata, int[] dictIds) {
      this.dictIdSet = new HashSet<Integer>(Math.max(1, dictIds.length));
      for (int dictId : dictIds) {
        dictIdSet.add(dictId);
      }
      valueIterator = (BlockSingleValIterator) blockValSet.iterator();
      setStartDocId(blockMetadata.getStartDocId());
      setEndDocId(blockMetadata.getEndDocId());
    }

    /**
     * After setting the startDocId, next calls will always return from >=startDocId
     * @param startDocId
     */
    public void setStartDocId(int startDocId) {
      this.startDocId = startDocId;
      valueIterator.skipTo(startDocId);
      currentDocId = startDocId;
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
      if (targetDocId < startDocId) {
        valueIterator.skipTo(startDocId);
        currentDocId = startDocId;
      } else {
        valueIterator.skipTo(targetDocId);
        currentDocId = targetDocId;
      }
      return next();
    }

    @Override
    public int next() {
      while (valueIterator.hasNext() && currentDocId <= endDocId) {
        int next = valueIterator.nextIntVal();
        if (dictIdSet.contains(next)) {
          return currentDocId++;
        }
        currentDocId = currentDocId + 1;
      }
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
