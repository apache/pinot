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
package org.apache.pinot.spi.utils;


/// Utilities for constructing SQL fragments. This stateless utility is thread-safe.
public class SqlUtils {
  private SqlUtils() {
  }

  /// Quotes one SQL identifier component with double quotes, escaping embedded double quotes by doubling them.
  /// Qualified names must be split by the caller so that each component is quoted separately.
  ///
  /// @param identifier SQL identifier component
  /// @return Identifier quoted for use in a SQL statement
  public static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
