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
package org.apache.calcite.sql.util;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ============================================================================
 * THIS CLASS IS COPIED FROM Calcite's {@link org.apache.calcite.sql.util.ChainedSqlOperatorTable} and modified the
 * function lookup to terminate early once found from ordered SqlOperatorTable list. This is to avoid some
 * hard-coded casting assuming all Sql identifier looked-up are of the same SqlOperator type.
 * ============================================================================
 *
 * PinotChainedSqlOperatorTable implements the {@link SqlOperatorTable} interface by
 * chaining together any number of underlying operator table instances.
 */
public class PinotChainedSqlOperatorTable implements SqlOperatorTable {
  //~ Instance fields --------------------------------------------------------

  protected final List<SqlOperatorTable> _tableList;

  //~ Constructors -----------------------------------------------------------

  public PinotChainedSqlOperatorTable(List<SqlOperatorTable> tableList) {
    this(ImmutableList.copyOf(tableList));
  }

  /** Internal constructor; call {@link SqlOperatorTables#chain}. */
  protected PinotChainedSqlOperatorTable(ImmutableList<SqlOperatorTable> tableList) {
    _tableList = ImmutableList.copyOf(tableList);
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public void add(SqlOperatorTable table) {
    if (!_tableList.contains(table)) {
      _tableList.add(table);
    }
  }

  @Override public void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    for (SqlOperatorTable table : _tableList) {
      table.lookupOperatorOverloads(opName, category, syntax, operatorList,
          nameMatcher);
      // ====================================================================
      // LINES CHANGED BELOW
      // ====================================================================
      if (!operatorList.isEmpty()) {
        break;
      }
      // ====================================================================
      // LINES CHANGED ABOVE
      // ====================================================================
    }
  }

  @Override public List<SqlOperator> getOperatorList() {
    List<SqlOperator> list = new ArrayList<>();
    for (SqlOperatorTable table : _tableList) {
      list.addAll(table.getOperatorList());
    }
    return list;
  }
}
