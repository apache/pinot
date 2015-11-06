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
package com.linkedin.pinot.core.common;

import java.util.ArrayList;
import java.util.List;

public class CompositeBaseFilterBlock extends BaseFilterBlock {
  private final List<BaseFilterBlock> blocks;

  public CompositeBaseFilterBlock(List<BaseFilterBlock> blocks) {
    this.blocks = blocks;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    Integer minDocId = null;
    Integer maxDocId = null;
    final List<FilterBlockDocIdSet> filterBlocks = new ArrayList<>(blocks.size());
    for (BaseFilterBlock block : blocks) {
      FilterBlockDocIdSet filterBlock = block.getFilteredBlockDocIdSet();
      if (minDocId == null || filterBlock.getMinDocId() < minDocId) {
        minDocId = filterBlock.getMinDocId();
      }
      if (maxDocId == null || filterBlock.getMaxDocId() > maxDocId) {
        maxDocId = filterBlock.getMaxDocId();
      }
      filterBlocks.add(filterBlock);
    }

    final int finalMinDocId = minDocId == null ? 0 : minDocId;
    final int finalMaxDocId = maxDocId == null ? 0 : maxDocId;

    return new FilterBlockDocIdSet() {
      @Override
      public int getMinDocId() {
        return finalMinDocId;
      }

      @Override
      public int getMaxDocId() {
        return finalMaxDocId;
      }

      @Override
      public void setStartDocId(int startDocId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void setEndDocId(int endDocId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          int blockIdx = 0;
          BlockDocIdIterator itr = null;

          @Override
          public int currentDocId() {
            checkIterator();
            return itr.currentDocId();
          }

          @Override
          public int next() {
            checkIterator();
            int next = itr.next();
            while (next == Constants.EOF && blockIdx < filterBlocks.size()) {
              checkIterator();
              next = itr.next();
            }
            return next;
          }

          @Override
          public int advance(int targetDocId) {
            do {
              // We loop here because we may be trying to advance to a target doc ID pas the current sub-segment
              checkIterator();
              itr.advance(targetDocId);
            } while (itr.currentDocId() == Constants.EOF && blockIdx < filterBlocks.size());
            return next();
          }

          void checkIterator() {
            if (itr == null) {
              itr = filterBlocks.get(blockIdx++).iterator();
            }

            while (itr.currentDocId() == Constants.EOF && blockIdx < filterBlocks.size()) {
              itr = filterBlocks.get(blockIdx++).iterator();
            }
          }
        };
      }

      @Override
      public <T> T getRaw() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public BlockId getId() {
    return new BlockId(0);
  }
}
