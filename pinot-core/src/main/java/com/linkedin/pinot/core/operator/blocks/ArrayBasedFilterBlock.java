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

import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.operator.docidsets.ArrayBasedDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;

public final class ArrayBasedFilterBlock extends BaseFilterBlock {
  private final int[] docIdArray;

  public ArrayBasedFilterBlock(int[] docIdArray) {
    this.docIdArray = docIdArray;
  }

  @Override
  public BlockId getId() {
    return new BlockId(0);
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    return new ArrayBasedDocIdSet(docIdArray, docIdArray.length);
  }
}