/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.blocks;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * InstanceResponseBlock is just a holder to get InstanceResponse from InstanceResponseBlock.
 */
public class InstanceResponseBlock implements Block {
  private final QueryContext _queryContext;
  private final BaseResultsBlock _baseResultsBlock;
  private final Map<String, String> _metadata;
  private final Map<Integer, String> _exceptionMap;

  public InstanceResponseBlock(BaseResultsBlock baseResultsBlock, QueryContext queryContext) {
    this(baseResultsBlock, queryContext, baseResultsBlock.computeMetadata());
  }

  public InstanceResponseBlock(BaseResultsBlock baseResultsBlock, QueryContext queryContext,
      Map<String, String> metadata) {
    _queryContext = queryContext;
    _baseResultsBlock = baseResultsBlock;
    _metadata = metadata;
    _exceptionMap = new HashMap<>();
  }

  public InstanceResponseBlock(Map<String, String> metadata) {
    _queryContext = null;
    _baseResultsBlock = null;
    _metadata = metadata;
    _exceptionMap = new HashMap<>();
  }

  public InstanceResponseBlock() {
    _queryContext = null;
    _baseResultsBlock = null;
    _metadata = new HashMap<>();
    _exceptionMap = new HashMap<>();
  }

  public QueryContext getQueryContext() {
    return _queryContext;
  }

  public BaseResultsBlock getBaseResultsBlock() {
    return _baseResultsBlock;
  }

  public Map<String, String> getInstanceResponseMetadata() {
    return _metadata;
  }

  public Map<Integer, String> getExceptionMap() {
    return _exceptionMap;
  }

  public void addException(ProcessingException exception) {
    _exceptionMap.put(exception.getErrorCode(), exception.getMessage());
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }
}
