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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


/**
 * In columnar data store, we don't want to filter multiple times for multiple
 * DataSources. So we use BDocIdSetOperator as a cache of docIdSetBlock. So
 * DocIdSet is re-accessable for multiple DataSources.
 *
 * BReplicatedDocIdSetOperator will take BDocIdSetOperator as input and is the
 * input for ColumnarReaderDataSource.
 * It will always return the current block from BDocIdSetOperator.
 * So ALWAYS call BDocIdSetOperator.nextBlock() BEFORE calling this class.
 *
 *
 *
 */
public class UReplicatedDocIdSetOperator extends BaseOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UReplicatedDocIdSetOperator.class);

  private final BReusableFilteredDocIdSetOperator _docIdSetOperator;

  public UReplicatedDocIdSetOperator(Operator docIdSetOperator) {
    _docIdSetOperator = (BReusableFilteredDocIdSetOperator) docIdSetOperator;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block getNextBlock() {
    return _docIdSetOperator.getCurrentDocIdSetBlock();
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "UReplicatedDocIdSetOperator";
  }

  @Override
  public boolean close() {
    return true;
  }

}
