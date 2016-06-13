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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.StarTreeDocIdSet;
import com.linkedin.pinot.core.operator.filter.AndOperator;

public class StarTreeFilterBlock extends BaseFilterBlock {
  private final int minDocId;
  private final int maxDocId;
  private final Operator filter;

  public StarTreeFilterBlock(int minDocId, int maxDocId, Operator filter) {
    this.minDocId = minDocId;
    this.maxDocId = maxDocId;
    this.filter = filter;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    if (filter == null) {
      StarTreeDocIdSet docIdSet = new StarTreeDocIdSet();
      docIdSet.setStartDocId(minDocId);
      docIdSet.setEndDocId(maxDocId);
      return docIdSet;
    } else {
      Block block = filter.nextBlock();
      FilterBlockDocIdSet docIdSet = (FilterBlockDocIdSet) block.getBlockDocIdSet();
      docIdSet.setStartDocId(minDocId);
      docIdSet.setEndDocId(maxDocId);
      return docIdSet;
    }
  }

  @Override
  public BlockId getId() {
    return new BlockId(0);
  }
}
