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

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * Utility functionality to maintain compatibility between the desired response format
 * and internal v1 server format for GROUP BY queries:
 * <ul>
 *   <li>{@link org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter}
 *   will return a result format that is not identical to the original request. This compat
 *   utility will rewrite those blocks into the correct format.</li>
 * </ul>
 */
public class LeafDistinctCompatibilityOperator extends BaseOperator<TransferableBlock> {

  private final Operator<TransferableBlock> _child;
  private final QueryContext _queryContext;
  private final DataSchema _targetDataSchema;

  public LeafDistinctCompatibilityOperator(Operator<TransferableBlock> child, QueryContext queryContext,
      DataSchema targetDataSchema) {
    _child = child;
    _queryContext = queryContext;
    _targetDataSchema = targetDataSchema;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    TransferableBlock transferableBlock = _child.nextBlock();
    if (TransferableBlockUtils.isEndOfStream(transferableBlock)) {
      return transferableBlock;
    }

    DistinctTable distinctTable = ObjectSerDeUtils.deserialize(transferableBlock.getDataBlock().getCustomObject(0, 0));
    DistinctTable mainTable = new DistinctTable(distinctTable.getDataSchema(), null,
        DistinctExecutor.MAX_INITIAL_CAPACITY, false);

    mainTable.mergeTable(distinctTable);

    List<Object[]> rows = mainTable.reduceToResultTable().getRows();
    return new TransferableBlock(rows, _targetDataSchema, BaseDataBlock.Type.ROW);
  }

  @Override
  public List<Operator> getChildOperators() {
    return ImmutableList.of(_child);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return "LEAF_GROUP_COMPAT";
  }
}
