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
    int currentDocId = -1;
    private Set<Integer> dictIdSet;
    final int[] intArray;
    private int startDocId;
    private int endDocId;

    public BlockValSetBlockDocIdIterator(BlockValSet blockValSet, BlockMetadata blockMetadata, int[] dictIds) {
      if (dictIds.length > 0) {
        this.dictIdSet = new HashSet<Integer>(dictIds.length);
        for (int dictId : dictIds) {
          dictIdSet.add(dictId);
        }
        this.intArray = new int[blockMetadata.getMaxNumberOfMultiValues()];
        Arrays.fill(intArray, 0);
        setStartDocId(blockMetadata.getStartDocId());
        setEndDocId(blockMetadata.getEndDocId());
      } else {
        this.dictIdSet = null;
        this.intArray = new int[0];
        setStartDocId(Constants.EOF);
        setEndDocId(Constants.EOF);
        currentDocId = Constants.EOF;
      }
      valueIterator = (BlockMultiValIterator) blockValSet.iterator();
    }

    /**
     * After setting the startDocId, next calls will always return from >=startDocId 
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
        int length = valueIterator.nextIntVal(intArray);
        boolean found = false;
        for (int i = 0; i < length; i++) {
          if (dictIdSet.contains(intArray[i])) {
            found = true;
            break;
          }
        }
        if (found) {
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
