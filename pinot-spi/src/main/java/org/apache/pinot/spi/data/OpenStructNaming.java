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

  /// Returns true if the given column name is a materialized OPEN_STRUCT child column
  /// (dense materialized key or the sparse JSON column).
  public static boolean isMaterializedOpenStructColumn(String columnName) {
    return columnName.indexOf(SEPARATOR.charAt(0)) > 0;
  }

  /// Returns true if the given column name is the sparse JSON column for some
  /// OPEN_STRUCT parent.
  public static boolean isSparseColumn(String columnName) {
    int sep = columnName.indexOf(SEPARATOR.charAt(0));
    return sep > 0 && SPARSE_SUFFIX.equals(columnName.substring(sep + 1));
  }

  /// Returns the parent OPEN_STRUCT column name for a materialized child column.
  /// Throws IllegalArgumentException if the input is not a materialized child column.
  public static String parseParentColumn(String materializedColumnName) {
    int sep = materializedColumnName.indexOf(SEPARATOR.charAt(0));
    if (sep <= 0) {
      throw new IllegalArgumentException("Not a materialized OPEN_STRUCT column: " + materializedColumnName);
    }
    return materializedColumnName.substring(0, sep);
  }

  /// Returns the key portion of a materialized dense column name. Throws
  /// IllegalArgumentException for the sparse column or non-materialized names.
  public static String parseKey(String materializedColumnName) {
    int sep = materializedColumnName.indexOf(SEPARATOR.charAt(0));
    if (sep <= 0) {
      throw new IllegalArgumentException("Not a materialized OPEN_STRUCT column: " + materializedColumnName);
    }
    String key = materializedColumnName.substring(sep + 1);
    if (SPARSE_SUFFIX.equals(key)) {
      throw new IllegalArgumentException("Sparse column has no key: " + materializedColumnName);
    }
    return key;
  }
}
