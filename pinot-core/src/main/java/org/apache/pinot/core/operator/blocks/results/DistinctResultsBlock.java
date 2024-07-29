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
package org.apache.pinot.core.operator.blocks.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * Results block for distinct queries.
 */
public class DistinctResultsBlock extends BaseResultsBlock {
  private final QueryContext _queryContext;

  private DistinctTable _distinctTable;

  public DistinctResultsBlock(DistinctTable distinctTable, QueryContext queryContext) {
    _distinctTable = distinctTable;
    _queryContext = queryContext;
  }

  public DistinctTable getDistinctTable() {
    return _distinctTable;
  }

  public void setDistinctTable(DistinctTable distinctTable) {
    _distinctTable = distinctTable;
  }

  @Override
  public int getNumRows() {
    return _distinctTable.size();
  }

  @Override
  public QueryContext getQueryContext() {
    return _queryContext;
  }

  @Override
  public DataSchema getDataSchema() {
    return _distinctTable.getDataSchema();
  }

  @Override
  public List<Object[]> getRows() {
    List<Object[]> rows = new ArrayList<>(_distinctTable.size());
    for (Record record : _distinctTable.getRecords()) {
      rows.add(record.getValues());
    }
    return rows;
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    Collection<Object[]> rows = getRows();
    return SelectionOperatorUtils.getDataTableFromRows(rows, _distinctTable.getDataSchema(),
        _queryContext.isNullHandlingEnabled());
  }
}
