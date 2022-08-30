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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Results block for distinct queries.
 */
public class DistinctResultsBlock extends BaseResultsBlock {
  private final DistinctAggregationFunction _distinctFunction;
  private DistinctTable _distinctTable;

  public DistinctResultsBlock(DistinctAggregationFunction distinctFunction, DistinctTable distinctTable) {
    _distinctFunction = distinctFunction;
    _distinctTable = distinctTable;
  }

  public DistinctTable getDistinctTable() {
    return _distinctTable;
  }

  public void setDistinctTable(DistinctTable distinctTable) {
    _distinctTable = distinctTable;
  }

  @Override
  public DataTable getDataTable(QueryContext queryContext)
      throws Exception {
    String[] columnNames = new String[]{_distinctFunction.getColumnName()};
    ColumnDataType[] columnDataTypes = new ColumnDataType[]{ColumnDataType.OBJECT};
    DataTableBuilder dataTableBuilder =
        DataTableFactory.getDataTableBuilder(new DataSchema(columnNames, columnDataTypes));
    dataTableBuilder.startRow();
    dataTableBuilder.setColumn(0, _distinctTable);
    dataTableBuilder.finishRow();
    DataTable dataTable = dataTableBuilder.build();
    attachMetadataToDataTable(dataTable);
    return dataTable;
  }
}
