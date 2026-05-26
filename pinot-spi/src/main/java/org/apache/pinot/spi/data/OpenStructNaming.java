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
package org.apache.pinot.spi.data;


/// Naming convention for OPEN_STRUCT materialized columns. Each dense OPEN_STRUCT key is stored as
/// a column named `<openStructColumn>$<key>`. Sparse keys share a single synthetic JSON column
/// named `<openStructColumn>$__sparse__`.
///
/// **Caller contract:** all parsing and classification helpers (`isMaterializedOpenStructColumn`,
/// `parseOpenStructColumn`, `parseKey`, `isSparseColumn`) assume the column name comes from a
/// schema where the OPEN_STRUCT index has been validated. `TableConfigUtils.validate()` rejects
/// `$` in user-defined column names when the OPEN_STRUCT index is enabled, which guarantees that
/// the first `$` in a column name is the map/key separator. Calling these helpers on
/// arbitrary column names — including from OPEN_STRUCT-disabled tables — may misclassify or
/// mis-parse names that legally contain `$`.
public final class OpenStructNaming {
  public static final String SEPARATOR = "$";
  public static final String SPARSE_SUFFIX = "__sparse__";

  private OpenStructNaming() {
  }

  public static String materializedColumnName(String openStructColumn, String key) {
    return openStructColumn + SEPARATOR + key;
  }

  public static String sparseColumnName(String openStructColumn) {
    return openStructColumn + SEPARATOR + SPARSE_SUFFIX;
  }

  /// Returns `true` if the column name follows the materialized-OPEN_STRUCT shape `<openStructColumn>$<key>`.
  /// **Only meaningful when called on columns from a OPEN_STRUCT-validated schema** — see the class-level
  /// caller contract.
  public static boolean isMaterializedOpenStructColumn(String columnName) {
    return columnName.contains(SEPARATOR);
  }

  public static boolean isSparseColumn(String columnName) {
    return columnName.endsWith(SEPARATOR + SPARSE_SUFFIX);
  }

  /// Returns the parent OPEN_STRUCT column name from a materialized column. Result is the original
  /// string when no `$` is present. **Caller must verify `isMaterializedOpenStructColumn(name)` first**
  /// when operating on arbitrary column names; see the class-level caller contract.
  public static String parseOpenStructColumn(String materializedColumnName) {
    int idx = materializedColumnName.indexOf(SEPARATOR);
    return idx >= 0 ? materializedColumnName.substring(0, idx) : materializedColumnName;
  }

  /// Returns the OPEN_STRUCT key from a materialized column. Result is the original string when no
  /// `$` is present. **Caller must verify `isMaterializedOpenStructColumn(name)` first** when operating
  /// on arbitrary column names; see the class-level caller contract.
  public static String parseKey(String materializedColumnName) {
    int idx = materializedColumnName.indexOf(SEPARATOR);
    return idx >= 0 ? materializedColumnName.substring(idx + SEPARATOR.length()) : materializedColumnName;
  }
}
