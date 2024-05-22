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
package org.apache.pinot.spi.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;


/**
 * Implementation of the AuthorizationResult interface that provides authorization results
 * at the table level, including which tables failed authorization.
 */
public class TableAuthorizationResult implements AuthorizationResult {
  private Set<String> _failedTables;

  public TableAuthorizationResult() {
    _failedTables = new HashSet<>();
  }

  public TableAuthorizationResult(Set<String> failedTables) {
    setFailedTables(failedTables);
  }

  /**
   * Creates a TableAuthorizationResult with no failed tables.
   *
   * @return a TableAuthorizationResult with no failed tables.
   */
  public static TableAuthorizationResult noFailureResult() {
    return new TableAuthorizationResult();
  }

  @Override
  public boolean hasAccess() {
    return _failedTables.isEmpty();
  }

  public Set<String> getFailedTables() {
    return _failedTables;
  }

  public void setFailedTables(Set<String> failedTables) {
    _failedTables = new HashSet<>(failedTables);
    ;
  }

  public void addFailedTable(String failedTable) {
    _failedTables.add(failedTable);
  }

  /**
   * Provides the failure message indicating which tables failed authorization.
   *
   * @return a string containing the failure message if there are failed tables, otherwise an empty string.
   */
  @Override
  public String getFailureMessage() {
    if (hasAccess()) {
      return StringUtils.EMPTY;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Authorization Failed for tables: ");
    // sort _failedTables into a list

    List<String> failedTablesList = new ArrayList<>(_failedTables);
    Collections.sort(failedTablesList); // Sort to make output deterministic

    for (String table : failedTablesList) {
      sb.append(table);
      sb.append(", ");
    }
    return sb.toString().trim();
  }
}
