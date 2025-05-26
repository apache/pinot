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

import java.util.Optional;
import java.util.Set;


public interface MultiTableAuthResult {
  /**
   * Indicates whether the access is granted.
   *
   * @return true if access is granted, false otherwise.
   */
  boolean hasAccess();

  /**
   * Checks auth status of a single table.
   * @param tableName the table name.
   * @return empty optional if that table is not present, optional of the auth status otherwise
   */
  Optional<Boolean> hasAccess(String tableName);

  /**
   * Returns the set of tables that failed auth.
   * @return the set of tables that failed auth
   */
  Set<String> getFailedTables();

  /**
   * Provides the failure message if access is denied.
   *
   * @return A string containing the failure message if access is denied, otherwise an empty string or null.
   */
  String getFailureMessage();
}
