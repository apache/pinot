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

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;


public class EmptyDistinctTable extends DistinctTable {

  public EmptyDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled) {
    super(dataSchema, limit, nullHandlingEnabled);
  }

  @Override
  public boolean hasOrderBy() {
    throw new UnsupportedOperationException();
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
  public int size() {
    return 0;
  }

  @Override
  public boolean isSatisfied() {
    return false;
  }

  @Override
  public List<Object[]> getRows() {
    return List.of();
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    return DataTableBuilderFactory.getDataTableBuilder(_dataSchema).build();
  }

  @Override
  public ResultTable toResultTable() {
    return new ResultTable(_dataSchema, List.of());
  }
}
