/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.MatchEntireSegmentDocIdSetBlock;


public class MatchEntireSegmentOperator implements Operator {

  private int docs;
  private int nextBlockCallCounter = 0;

  public MatchEntireSegmentOperator(int docs) {
    this.docs = docs;

  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public Block nextBlock() {
    return nextBlock(new BlockId(0));
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    if (nextBlockCallCounter > 0) {
      return null;
    }
    MatchEntireSegmentDocIdSetBlock block = new MatchEntireSegmentDocIdSetBlock(docs);
    nextBlockCallCounter = nextBlockCallCounter + 1;
    return block;
  }
}
