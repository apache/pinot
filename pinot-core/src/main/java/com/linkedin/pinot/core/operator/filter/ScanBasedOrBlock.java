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


public class ScanBasedOrBlock implements Block {

  private static Logger LOGGER = Logger.getLogger(ScanBasedOrBlock.class);

  private final Block[] _scanBasedBlocks;

  public ScanBasedOrBlock(Block[] blocks) {
    _scanBasedBlocks = blocks;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException("Not support getId() in ScanBasedOrBlock");
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException("Not support getMetadata() in ScanBasedOrBlock");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("Not support getBlockValueSet() in ScanBasedOrBlock");
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Not support getBlockDocIdValueSet() in ScanBasedOrBlock");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    final BlockDocIdSet[] blockDocIdSets = new BlockDocIdSet[_scanBasedBlocks.length];
    for (int i = 0; i < _scanBasedBlocks.length; ++i) {
      if (_scanBasedBlocks[i] != null) {
        blockDocIdSets[i] = _scanBasedBlocks[i].getBlockDocIdSet();
      }
    }
    return new BlockDocIdSet() {
      @Override
      public BlockDocIdIterator iterator() {
        return new OrBlockDocIdIterator(blockDocIdSets);
      }

      @Override
      public Object getRaw() {
        throw new UnsupportedOperationException("Not support getRaw() in ScanBasedOrBlock.getBlockDocIdSet()");
      }
    };
  }

}
