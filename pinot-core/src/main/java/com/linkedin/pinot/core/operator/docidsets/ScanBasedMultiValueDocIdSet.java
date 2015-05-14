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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;


public class ScanBasedMultiValueDocIdSet implements FilterBlockDocIdSet {
  private final BlockValSet blockValSet;
  private BlockMetadata blockMetadata;
  private BlockValSetBlockDocIdIterator blockValSetBlockDocIdIterator;

  public ScanBasedMultiValueDocIdSet(BlockValSet blockValSet, BlockMetadata blockMetadata, int... dictIds) {
    this.blockValSet = blockValSet;
    this.blockMetadata = blockMetadata;
    blockValSetBlockDocIdIterator = new BlockValSetBlockDocIdIterator(blockValSet, blockMetadata, dictIds);
  }

  @Override
  public int getMinDocId() {
    return blockMetadata.getStartDocId();
  }

  @Override
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

  public static class BlockValSetBlockDocIdIterator implements BlockDocIdIterator {
    BlockMultiValIterator valueIterator;
    int counter = 0;
    private Set<Integer> dictIdSet;
    final int[] intArray;
    private int startDocId;
    private int endDocId;

    public BlockValSetBlockDocIdIterator(BlockValSet blockValSet, BlockMetadata blockMetadata, int[] dictIds) {
      this.dictIdSet = new HashSet<Integer>(Math.max(1, dictIds.length));
      for (int dictId : dictIds) {
        dictIdSet.add(dictId);
      }
      valueIterator = (BlockMultiValIterator) blockValSet.iterator();
      this.intArray = new int[blockMetadata.getMaxNumberOfMultiValues()];
      Arrays.fill(intArray, 0);
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
      counter = startDocId;
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
        counter = startDocId;
      } else {
        valueIterator.skipTo(targetDocId);
        counter = targetDocId;
      }
      return next();
    }

    @Override
    public int next() {
      while (valueIterator.hasNext() && counter <= endDocId) {
        int length = valueIterator.nextIntVal(intArray);
        boolean found = false;
        for (int i = 0; i < length; i++) {
          if (dictIdSet.contains(intArray[i])) {
            found = true;
            break;
          }
        }
        if (found) {
          return counter;
        }
        counter = counter + 1;
      }
      return (counter = Constants.EOF);
    }

    @Override
    public int currentDocId() {
      return counter;
    }
  };

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
  }
}
