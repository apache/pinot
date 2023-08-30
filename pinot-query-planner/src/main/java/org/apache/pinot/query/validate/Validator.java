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
package org.apache.pinot.query.validate;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;


/**
 * The {@code Validator} overwrites Calcite's Validator with Pinot specific logics.
 */
public class Validator extends SqlValidatorImpl {

  public Validator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
    // TODO: support BABEL validator. Currently parser conformance is set to use BABEL.
    super(opTab, catalogReader, typeFactory,
        Config.DEFAULT.withSqlConformance(SqlConformanceEnum.LENIENT).withIdentifierExpansion(true));
  }

  /**
   * Expand the star in the select list.
   * Pinot table schema has all columns along with virtual columns.
   * We don't want to include virtual columns in the select * query
   *
   * @param selectList        Select clause to be expanded
   * @param select             Query
   * @param includeSystemVars Whether to include system variables
   * @return Expanded select list
   */
  @Override
  public SqlNodeList expandStar(
      SqlNodeList selectList,
      SqlSelect select,
      boolean includeSystemVars) {
    SqlNodeList expandedSelectList = super.expandStar(selectList, select, includeSystemVars);
    RelRecordType validatedNodeType = (RelRecordType) getValidatedNodeType(select);

    List<String> selectedVirtualColumns = getSelectedVirtualColumns(select);
    // ExpandStar will add a field for each column in the table, but we don't want to include the virtual columns
    List<SqlNode> newSelectList = new ArrayList<>();
    List<RelDataTypeField> newFieldList = new ArrayList<>();
    for (int i = 0; i < expandedSelectList.size(); i++) {
      SqlNode node = expandedSelectList.get(i);
      if (node instanceof SqlIdentifier) {
        String columnName = getColumnName((SqlIdentifier) node);
        if (isVirtualColumn(columnName)) {
          // If the virtual column is selected, remove it from the list of selected virtual columns
          if (!selectedVirtualColumns.remove(columnName)) {
            continue;
          }
        }
      }
      newSelectList.add(node);
      newFieldList.add(validatedNodeType.getFieldList().get(i));
    }
    // Ensure the validation node type is updated
    setValidatedNodeType(select, new RelRecordType(newFieldList));
    return new SqlNodeList(newSelectList, selectList.getParserPosition());
  }

  /**
   * Get the all the virtual columns explicitly selected from the query selection list.
   * Use list in case there are duplicates.
   *
   * @param select query
   * @return list of virtual columns selected
   */
  private List<String> getSelectedVirtualColumns(SqlSelect select) {
    List<String> columnNamesFromSelectList = new ArrayList<>();
    for (SqlNode node : select.getSelectList().getList()) {
      if (node instanceof SqlIdentifier) {
        String columnName = getColumnName((SqlIdentifier) node);
        if (isVirtualColumn(columnName)) {
          columnNamesFromSelectList.add(columnName);
        }
      }
    }
    return columnNamesFromSelectList;
  }

  /**
   * Check if the column is a virtual column.
   * @param columnName column name
   * @return true if the column is a virtual column
   */
  private static boolean isVirtualColumn(String columnName) {
    return columnName.length() > 0 && columnName.charAt(0) == '$';
  }

  /**
   * Extract the column name from the identifier. The identifier could be in the format of [table].[columnName] or
   * just [columnName].
   * @param identifier column identifier
   * @return column name
   */
  private static String getColumnName(SqlIdentifier identifier) {
    List<String> names = identifier.names;
    return names.get(names.size() - 1);
  }
}
