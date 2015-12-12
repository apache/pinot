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

import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;

public class InvertedIndexBasedFilterOperator extends BaseFilterOperator{

  private DataSource dataSource;

  public InvertedIndexBasedFilterOperator(DataSource dataSource){
    this.dataSource = dataSource;
  }
  
  @Override
  public boolean open() {
    return true;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Skipping to a block Id is not supported yet");
  }

  @Override
  public boolean close() {
    return true;
  }

}
