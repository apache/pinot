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
package org.apache.pinot.query.runtime.operator.set;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Union operator for UNION queries. Unlike {@link UnionAllOperator}, this operator removes duplicate rows and only
 * returns distinct rows.
 */
public class UnionOperator extends RightRowSetBasedSetOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnionOperator.class);
  private static final String EXPLAIN_NAME = "UNION";

  public UnionOperator(OpChainExecutionContext opChainExecutionContext,
      List<MultiStageOperator> inputOperators, DataSchema dataSchema) {
    super(opChainExecutionContext, inputOperators, dataSchema);
  }

  @Override
  protected MseBlock processRightOperator() {
    MseBlock block = _rightChildOperator.nextBlock();
    while (block.isData()) {
      MseBlock.Data dataBlock = (MseBlock.Data) block;
      List<Object[]> rows = new ArrayList<>();
      for (Object[] row : dataBlock.asRowHeap().getRows()) {
        Record record = new Record(row);
        if (!_rightRowSet.contains(record)) {
          // Add a new unique row.
          rows.add(row);
          _rightRowSet.add(record);
        }
      }
      sampleAndCheckInterruption();
      // If we have collected some rows, return them as a new block.
      if (!rows.isEmpty()) {
        return new RowHeapDataBlock(rows, _dataSchema);
      } else {
        block = _rightChildOperator.nextBlock();
      }
    }
    assert block.isEos();
    return block;
  }

  @Override
  protected boolean handleRowMatched(Object[] row) {
    if (!_rightRowSet.contains(new Record(row))) {
      // Row is unique, add it to the result and also to the row set to skip later duplicates.
      _rightRowSet.add(new Record(row));
      return true;
    } else {
      // Row is a duplicate, skip it.
      return false;
    }
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.UNION;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
