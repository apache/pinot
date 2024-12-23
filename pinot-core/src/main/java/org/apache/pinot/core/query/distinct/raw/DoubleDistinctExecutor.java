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
package org.apache.pinot.core.query.distinct.raw;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.distinct.BaseSingleColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for single raw DOUBLE column.
 */
public class DoubleDistinctExecutor
    extends BaseSingleColumnDistinctExecutor<DoubleDistinctTable, double[], double[][]> {

  public DoubleDistinctExecutor(ExpressionContext expression, DataType dataType, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(expression, new DoubleDistinctTable(new DataSchema(new String[]{expression.toString()},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataType)}), limit, nullHandlingEnabled, orderByExpression));
  }

  @Override
  protected double[] getValuesSV(BlockValSet blockValSet) {
    return blockValSet.getDoubleValuesSV();
  }

  @Override
  protected double[][] getValuesMV(BlockValSet blockValSet) {
    return blockValSet.getDoubleValuesMV();
  }

  @Override
  protected boolean processSV(double[] values, int from, int to) {
    if (_distinctTable.hasLimit()) {
      if (_distinctTable.hasOrderBy()) {
        for (int i = from; i < to; i++) {
          _distinctTable.addWithOrderBy(values[i]);
        }
      } else {
        for (int i = from; i < to; i++) {
          if (_distinctTable.addWithoutOrderBy(values[i])) {
            return true;
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        _distinctTable.addUnbounded(values[i]);
      }
    }
    return false;
  }

  @Override
  protected boolean processMV(double[][] values, int from, int to) {
    if (_distinctTable.hasLimit()) {
      if (_distinctTable.hasOrderBy()) {
        for (int i = from; i < to; i++) {
          for (double value : values[i]) {
            _distinctTable.addWithOrderBy(value);
          }
        }
      } else {
        for (int i = from; i < to; i++) {
          for (double value : values[i]) {
            if (_distinctTable.addWithoutOrderBy(value)) {
              return true;
            }
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        for (double value : values[i]) {
          _distinctTable.addUnbounded(value);
        }
      }
    }
    return false;
  }
}
