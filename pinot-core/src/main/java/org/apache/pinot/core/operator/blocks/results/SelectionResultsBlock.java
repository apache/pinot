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
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * Results block for selection queries.
 */
public class SelectionResultsBlock extends BaseResultsBlock {
  private final DataSchema _dataSchema;
  private final Comparator<? super Object[]> _comparator;
  private List<Object[]> _rows;

  public SelectionResultsBlock(DataSchema dataSchema, List<Object[]> rows,
      @Nullable Comparator<? super Object[]> comparator) {
    _dataSchema = dataSchema;
    _rows = rows;
    _comparator = comparator;
  }

  public SelectionResultsBlock(DataSchema dataSchema, List<Object[]> rows) {
    this(dataSchema, rows, null);
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  public List<Object[]> getRows() {
    return _rows;
  }

  public void setRows(List<Object[]> rows) {
    _rows = rows;
  }

  @Nullable
  public Comparator<? super Object[]> getComparator() {
    return _comparator;
  }

  @Override
  public int getNumRows() {
    return _rows.size();
  }

  @Override
  public DataSchema getDataSchema(QueryContext queryContext) {
    return _dataSchema;
  }

  @Override
  public List<Object[]> getRows(QueryContext queryContext) {
    return _rows;
  }

  @Override
  public DataTable getDataTable(QueryContext queryContext)
      throws IOException {
    return SelectionOperatorUtils.getDataTableFromRows(_rows, _dataSchema, queryContext.isNullHandlingEnabled());
  }
}
