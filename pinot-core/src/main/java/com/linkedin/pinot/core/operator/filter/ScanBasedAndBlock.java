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
package com.linkedin.pinot.core.operator.filter;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


public class ScanBasedAndBlock implements Block {

  private static Logger LOGGER = Logger.getLogger(ScanBasedAndBlock.class);

  private final Block[] _scanBasedBlocks;

  public ScanBasedAndBlock(Block[] blocks) {
    _scanBasedBlocks = blocks;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException("Not support getId() in ScanBasedAndBlock");
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException("Not support getMetadata() in ScanBasedAndBlock");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("Not support getBlockValueSet() in ScanBasedAndBlock");
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Not support getBlockDocIdValueSet() in ScanBasedAndBlock");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    final BlockDocIdSet[] blockDocIdSets = new BlockDocIdSet[_scanBasedBlocks.length];
    for (int i = 0; i < _scanBasedBlocks.length; ++i) {
      blockDocIdSets[i] = _scanBasedBlocks[i].getBlockDocIdSet();
    }
    return new BlockDocIdSet() {
      @Override
      public BlockDocIdIterator iterator() {
        return new AndBlockDocIdIterator(blockDocIdSets);
      }

      @Override
      public Object getRaw() {
        throw new UnsupportedOperationException("Not support getRaw() in ScanBasedAndBlock.getBlockDocIdSet()");
      }
    };
  }

}
