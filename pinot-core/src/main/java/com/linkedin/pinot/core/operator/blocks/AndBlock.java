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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.docidsets.AndBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import java.util.List;


public class AndBlock extends BaseFilterBlock {

  final List<FilterBlockDocIdSet> blockDocIdSets;
  public AndBlockDocIdSet andBlockDocIdSet;

  public AndBlock(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.blockDocIdSets = blockDocIdSets;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    andBlockDocIdSet = new AndBlockDocIdSet(blockDocIdSets);
    return andBlockDocIdSet;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("Cannot apply predicate on a AND Block");
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Cannot apply predicate on a AND Block");
  }

  @Override
  public BlockMetadata getMetadata() {
    return null;
  }

}
