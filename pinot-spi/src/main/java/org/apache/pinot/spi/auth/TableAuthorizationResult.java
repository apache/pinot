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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


public class TableAuthorizationResult implements MultiTableAuthResult {

  private static final MultiTableAuthResult SUCCESS = new TableAuthorizationResult(Map.of());

  private final Map<String, Boolean> _authResult;
  private final Set<String> _failedTables;

  public TableAuthorizationResult(Map<String, Boolean> authResult) {
    _authResult = authResult;
    _failedTables =
        _authResult.entrySet().stream().filter(e -> !e.getValue()).map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  public TableAuthorizationResult(Set<String> failedTables) {
    Map<String, Boolean> authResult = new HashMap<>();
    for (String tableName : failedTables) {
      authResult.put(tableName, false);
    }
    _authResult = authResult;
    _failedTables = failedTables;
  }

  @Override
  public boolean hasAccess() {
    return _failedTables.isEmpty();
  }

  @Override
  public Optional<Boolean> hasAccess(String tableName) {
    return Optional.of(_authResult.get(tableName));
  }

  @Override
  public Set<String> getFailedTables() {
    return _failedTables;
  }

  @Override
  public String getFailureMessage() {
    if (hasAccess()) {
      return StringUtils.EMPTY;
    }

    List<String> failedTablesList = new ArrayList<>(_failedTables);
    Collections.sort(failedTablesList); // Sort to make output deterministic
    return "Authorization Failed for tables: " + failedTablesList;
  }

  public static MultiTableAuthResult success() {
    return SUCCESS;
  }
}
