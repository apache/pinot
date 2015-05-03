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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.AndBlock;
import com.linkedin.pinot.core.operator.docidsets.AndBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.OrBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedMultiValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedSingleValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;


public class AndOperator extends BaseFilterOperator {
  private static final Logger LOG = LoggerFactory.getLogger(AndOperator.class);

  private List<Operator> operators;
  private AndBlock andBlock;

  public AndOperator(List<Operator> operators) {
    this.operators = operators;
  }

  @Override
  public boolean open() {
    for (Operator operator : operators) {
      operator.open();
    }
    return true;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    List<BlockDocIdSet> blockDocIdSets = new ArrayList<BlockDocIdSet>();
    for (Operator operator : operators) {
      Block block = operator.nextBlock();
      BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      blockDocIdSets.add(blockDocIdSet);
    }
    andBlock = new AndBlock(blockDocIdSets);
    return andBlock;
  }

  @Override
  public boolean close() {
    for (Operator operator : operators) {
      operator.close();
    }
    LOG.info("Time spent in AND operator:{} is {}", this, andBlock.andBlockDocIdSet.timeMeasure);
    return true;
  }
}
