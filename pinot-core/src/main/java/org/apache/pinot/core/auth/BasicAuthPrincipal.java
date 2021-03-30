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
package org.apache.pinot.core.auth;

import java.util.Set;


/**
 * Container object for basic auth principal
 */
public class BasicAuthPrincipal {
  private final String _name;
  private final String _token;
  private final Set<String> _tables;
  private final Set<String> _permissions;

  public BasicAuthPrincipal(String name, String token, Set<String> tables, Set<String> permissions) {
    this._name = name;
    this._token = token;
    this._tables = tables;
    this._permissions = permissions;
  }

  public String getName() {
    return _name;
  }

  public String getToken() {
    return _token;
  }

  public boolean hasTable(String tableName) {
    return _tables.isEmpty() || _tables.contains(tableName);
  }

  public boolean hasPermission(String permission) {
    return _permissions.isEmpty() || _permissions.contains(permission);
  }
}
