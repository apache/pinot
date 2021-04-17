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
package org.apache.pinot.pql.parsers.pql2.ast;

/**
 * AST node for a table name.
 */
public class TableNameAstNode extends BaseAstNode {
  private String _tableName;
  private String _resourceName;

  public TableNameAstNode(String tableName) {
    if ((tableName.startsWith("'") && tableName.endsWith("'"))
        || (tableName.startsWith("\"") && tableName.endsWith("\""))) {
      tableName = tableName.substring(1, tableName.length() - 1);
    }
    int firstDotIndex = tableName.indexOf('.');
    if (firstDotIndex == -1) {
      _tableName = tableName;
      _resourceName = tableName;
    } else {
      _resourceName = tableName.substring(0, firstDotIndex);
      _tableName = tableName.substring(firstDotIndex + 1);
    }
  }

  public String getTableName() {
    return _tableName;
  }

  public String getResourceName() {
    return _resourceName;
  }
}
