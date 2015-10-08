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

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.StarTreeFilterBlock;

public class StarTreeOperator extends BaseFilterOperator {
  private final int minDocId;
  private final int maxDocId;
  private final Operator filter;

  public StarTreeOperator(int minDocId, int maxDocId, Operator filter) {
    this.minDocId = minDocId;
    this.maxDocId = maxDocId;
    this.filter = filter;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId blockId) {
    return new StarTreeFilterBlock(minDocId, maxDocId, filter);
  }

  @Override
  public boolean open() {
    if (filter != null) {
      filter.open();
    }
    return true;
  }

  @Override
  public boolean close() {
    if (filter != null) {
      filter.close();
    }
    return true;
  }
}
