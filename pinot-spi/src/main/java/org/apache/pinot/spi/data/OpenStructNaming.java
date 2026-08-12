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
/// [#metricKey] builds a superficially similar `<openStructColumn>$<key>` string for per-key metric
/// names, but escapes the key for JMX export and is not a column name -- see its docs.
public final class OpenStructNaming {
  public static final String SEPARATOR = "$";
  public static final String SPARSE_SUFFIX = "__sparse__";

  private OpenStructNaming() {
  }

  public static String materializedColumnName(String openStructColumn, String key) {
    return openStructColumn + SEPARATOR + key;
  }

  /// Builds the `<openStructColumn>$<key>` identifier used as the key segment of a per-key OPEN_STRUCT
  /// metric. Deliberately distinct from [#materializedColumnName]: it is only a metric identifier -- the
  /// scrape rule splits it back into `column` and `key` labels -- and the key it carries need not have a
  /// materialized column on disk. Its output is not a column name: do not feed it to [#parseKey] or
  /// [#parseParentColumn], which would hand back the still-escaped key.
  ///
  /// It also differs in escaping the key. OPEN_STRUCT keys come from user JSON, and the metric name is
  /// wrapped by `javax.management.ObjectName.quote` on the way to JMX, which backslash-escapes exactly
  /// `"`, `\`, `*` and `?`. An escaped `"` stops the name matching the per-key rule in
  /// `docker/images/pinot/etc/jmx_prometheus_javaagent/configs/server.yml` at all, so the key silently
  /// loses its `column`/`key` labels and falls through to a generic rule; the other three still match but
  /// leave a spurious backslash in the exported label value.
  ///
  /// Those four are percent-escaped rather than folded to `_`, which would not be injective -- `_` is a
  /// legal key character, so `a"b` and `a_b` would collapse onto one series and silently overwrite each
  /// other's gauge on every seal. `%` is escaped first to keep the mapping reversible. Nothing else is
  /// touched: `.`, `-`, `_`, `,`, `=` and spaces are all safe in a quoted `ObjectName` and in a
  /// Prometheus label value, so `user.id`, `user-id` and `user_id` stay distinct series.
  ///
  /// The escape set tracks the JMX export path rather than Pinot's registry abstraction -- it is exactly
  /// what `ObjectName.quote` escapes -- and would need revisiting if metrics stopped being exported
  /// through JMX.
  public static String metricKey(String openStructColumn, String key) {
    return openStructColumn + SEPARATOR + escapeKeyForMetricName(key);
  }

  private static String escapeKeyForMetricName(String key) {
    StringBuilder sb = null;
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      String escape = null;
      if (c == '%') {
        escape = "%25";
      } else if (c == '"') {
        escape = "%22";
      } else if (c == '\\') {
        escape = "%5C";
      } else if (c == '*') {
        escape = "%2A";
      } else if (c == '?') {
        escape = "%3F";
      }
      if (escape != null) {
        if (sb == null) {
          sb = new StringBuilder(key.length() + 8).append(key, 0, i);
        }
        sb.append(escape);
      } else if (sb != null) {
        sb.append(c);
      }
    }
    return sb == null ? key : sb.toString();
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
