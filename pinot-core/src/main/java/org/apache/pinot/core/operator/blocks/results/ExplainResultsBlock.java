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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Results block for EXPLAIN queries.
 */
public class ExplainResultsBlock extends BaseResultsBlock {
  private final List<ExplainEntry> _entries = new ArrayList<>();

  public void addOperator(String operatorName, int operatorId, int parentId) {
    _entries.add(new ExplainEntry(operatorName, operatorId, parentId));
  }

  @Override
  public DataSchema getDataSchema(QueryContext queryContext) {
    return DataSchema.EXPLAIN_RESULT_SCHEMA;
  }

  @Override
  public int getNumRows() {
    return _entries.size();
  }

  @Override
  public List<Object[]> getRows(QueryContext queryContext) {
    List<Object[]> rows = new ArrayList<>(_entries.size());
    for (ExplainEntry entry : _entries) {
      rows.add(entry.toRow());
    }
    return rows;
  }

  @Override
  public DataTable getDataTable(QueryContext queryContext)
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(DataSchema.EXPLAIN_RESULT_SCHEMA);
    for (ExplainEntry entry : _entries) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, entry._operatorName);
      dataTableBuilder.setColumn(1, entry._operatorId);
      dataTableBuilder.setColumn(2, entry._parentId);
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }

  @Override
  public Map<String, String> getResultsMetadata() {
    // Do not add metadata for EXPLAIN results
    return new HashMap<>();
  }

  private static class ExplainEntry {
    final String _operatorName;
    final int _operatorId;
    final int _parentId;

    ExplainEntry(String operatorName, int operatorId, int parentId) {
      _operatorName = operatorName;
      _operatorId = operatorId;
      _parentId = parentId;
    }

    Object[] toRow() {
      return new Object[]{_operatorName, _parentId, _parentId};
    }
  }
}
