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

import com.linkedin.pinot.core.operator.docidsets.CompositeFilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import java.util.ArrayList;
import java.util.List;

public class CompositeBaseFilterBlock extends BaseFilterBlock {
  private final List<BaseFilterBlock> blocks;

  public CompositeBaseFilterBlock(List<BaseFilterBlock> blocks) {
    this.blocks = blocks;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    List<FilterBlockDocIdSet> filteredDocIdSets = new ArrayList<>(blocks.size());
    for (BaseFilterBlock block : blocks) {
      filteredDocIdSets.add(block.getFilteredBlockDocIdSet());
    }
    return new CompositeFilterBlockDocIdSet(filteredDocIdSets);
  }
}
