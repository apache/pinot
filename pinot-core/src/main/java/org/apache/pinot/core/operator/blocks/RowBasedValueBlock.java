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

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.reduce.RowBasedBlockValSet;


public class RowBasedValueBlock extends RowBlock implements ValueBlock {

  public RowBasedValueBlock(DataSchema dataSchema, List<Object[]> rows) {
    super(dataSchema, rows);
  }

  @Override
  public int getNumDocs() {
    return _rows.size();
  }

  @Nullable
  @Override
  public int[] getDocIds() {
    return null;
  }

  @Override
  public BlockValSet getBlockValueSet(ExpressionContext expression) {
    Preconditions.checkState(expression.getType() == ExpressionContext.Type.IDENTIFIER,
        "Only support reading column (IDENTIFIER) from RowBlock, got: %s", expression.getType());
    return getBlockValueSet(expression.getIdentifier());
  }

  @Override
  public BlockValSet getBlockValueSet(String column) {
    // TODO: Consider caching the RowBasedBlockValSet and reuse the column major value array
    int index = ArrayUtils.indexOf(_dataSchema.getColumnNames(), column);
    Preconditions.checkState(index >= 0, "Failed to find column: %s", column);
    DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataTypes()[index];
    Preconditions.checkState(!columnDataType.isArray(), "Do not support ARRAY type: %s", columnDataType);
    return new RowBasedBlockValSet(columnDataType, _rows, index);
  }
}
