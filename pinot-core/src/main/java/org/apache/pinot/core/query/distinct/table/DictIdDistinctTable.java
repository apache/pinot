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
package org.apache.pinot.core.query.distinct.table;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;


public class DictIdDistinctTable extends IntDistinctTable {

  public DictIdDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(dataSchema, limit, nullHandlingEnabled, orderByExpression);
  }

  public IntOpenHashSet getValueSet() {
    return _valueSet;
  }

  @Nullable
  public OrderByExpressionContext getOrderByExpression() {
    return _orderByExpression;
  }

  @Override
  protected IntComparator getComparator(OrderByExpressionContext orderByExpression) {
    return orderByExpression.isAsc() ? (v1, v2) -> v2 - v1 : (v1, v2) -> v1 - v2;
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mergeDataTable(DataTable dataTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getRows() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultTableRows toResultTable() {
    throw new UnsupportedOperationException();
  }
}
