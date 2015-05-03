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
package com.linkedin.pinot.core.operator;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;


/**
 * UReplicatedProjectionOperator is used by AggregationFunctionOperator and
 * AggregationFunctionGroupByOperator as a copy of MProjectionOperator.
 * nextBlock() here returns currentBlock in MProjectionOperator.
 *
 * @author xiafu
 *
 */
public class UReplicatedProjectionOperator implements Operator {

  private static final Logger LOG = Logger.getLogger(UReplicatedProjectionOperator.class);

  private final MProjectionOperator _projectionOperator;

  public UReplicatedProjectionOperator(MProjectionOperator projectionOperator) {
    _projectionOperator = projectionOperator;
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }

  @Override
  public Block nextBlock() {
    return _projectionOperator.getCurrentBlock();
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not supported in MProjectionOperator!");
  }

  public MProjectionOperator getProjectionOperator() {
    return _projectionOperator;
  }
  
}
