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
package org.apache.pinot.sql.ddl.reverse;

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;


/// Quoting helpers used by the canonical DDL emitter.
final class SqlIdentifiers {

  /// SQL identifiers that need double-quoting because they conflict with the Pinot DDL grammar
  /// (reserved or context-sensitive keywords) or because they are not safe bare identifiers.
  ///
  /// This set is intentionally over-cautious: a few extra quotes never break round-trip,
  /// but a missing quote would cause a re-parse to misinterpret the identifier as a keyword.
  private static final Set<String> ALWAYS_QUOTE = ImmutableSet.of(
      // grammar keywords introduced by Pinot DDL that could appear as user identifiers
      "DIMENSION", "METRIC", "DATETIME", "FORMAT", "GRANULARITY", "OFFLINE", "REALTIME",
      "PROPERTIES", "TABLES", "TABLE_TYPE", "IF", "TYPE", "FROM",
      // standard SQL keywords that round-trip incorrectly when bare
      "TABLE", "CREATE", "DROP", "SHOW", "NOT", "NULL", "DEFAULT", "EXISTS", "AS", "BY",
      "ORDER", "PRIMARY", "KEY", "WITH", "ON", "AND", "OR", "SELECT", "WHERE",
      // data-type keywords consumed by the column-declaration grammar's DataType() rule.
      // Without quoting, a column literally named e.g. "int" would re-parse as a type token
      // rather than an identifier and the column declaration would fail.
      "INT", "INTEGER", "SMALLINT", "TINYINT", "BIGINT", "LONG", "FLOAT", "REAL", "DOUBLE",
      "DECIMAL", "NUMERIC", "BIG_DECIMAL", "BOOLEAN", "TIMESTAMP", "VARCHAR", "CHAR", "STRING",
      "VARBINARY", "BINARY", "BYTES", "JSON");

  private static final Pattern BARE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private SqlIdentifiers() {
  }

  /// Returns `identifier` ready to embed in canonical DDL: double-quoted if required,
  /// bare otherwise. Embedded double quotes are escaped per SQL convention (`"` → `""`).
  static String quote(String identifier) {
    if (mustQuote(identifier)) {
      return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
    return identifier;
  }

  /// Returns a single-quoted SQL string literal for `value`; embedded single quotes are
  /// doubled per SQL convention.
  static String quoteString(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static boolean mustQuote(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return true;
    }
    if (!BARE_IDENTIFIER.matcher(identifier).matches()) {
      return true;
    }
    return ALWAYS_QUOTE.contains(identifier.toUpperCase(Locale.ROOT));
  }
}
