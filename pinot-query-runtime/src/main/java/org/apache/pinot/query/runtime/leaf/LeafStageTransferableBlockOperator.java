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
package org.apache.pinot.query.runtime.leaf;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * Leaf-stage transfer block opreator is used to wrap around the leaf stage process results. They are passed to the
 * Pinot server to execute query thus only one {@link DataTable} were returned. However, to conform with the
 * intermediate stage operators. an additional {@link MetadataBlock} needs to be transfer after the data block.
 *
 * <p>In order to achieve this:
 * <ul>
 *   <li>The leaf-stage result is split into data payload block and metadata payload block.</li>
 *   <li>In case the leaf-stage result contains error or only metadata, we skip the data payload block.</li>
 * </ul>
 */
public class LeafStageTransferableBlockOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

  private final BaseDataBlock _errorBlock;
  private final List<BaseDataBlock> _baseDataBlocks;
  private final DataSchema _dataSchema;
  private boolean _hasTransferred;
  private int _currentIndex;

  public LeafStageTransferableBlockOperator(List<BaseDataBlock> baseDataBlocks, DataSchema dataSchema) {
    _baseDataBlocks = baseDataBlocks;
    _dataSchema = dataSchema;
    _errorBlock = baseDataBlocks.stream().filter(e -> !e.getExceptions().isEmpty()).findFirst().orElse(null);
    _currentIndex = 0;
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_currentIndex < 0) {
      throw new RuntimeException("Leaf transfer terminated. next block should no longer be called.");
    }
    if (_errorBlock != null) {
      _currentIndex = -1;
      return new TransferableBlock(_errorBlock);
    } else {
      if (_currentIndex < _baseDataBlocks.size()) {
        return new TransferableBlock(_baseDataBlocks.get(_currentIndex++));
      } else {
        _currentIndex = -1;
        return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(_dataSchema));
      }
    }
  }
}
