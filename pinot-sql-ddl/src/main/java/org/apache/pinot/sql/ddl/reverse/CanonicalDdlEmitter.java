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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Renders a `CREATE TABLE` statement in canonical Pinot DDL form from a [Schema] and
/// [TableConfig]. Designed so that `parse(emit(schema, config))` round-trips back to
/// a semantically-equivalent (Schema, TableConfig) pair.
///
/// Canonical formatting rules:
/// - Two-space indentation, one column per line, trailing comma between entries.
/// - Clause order: column block, `TABLE_TYPE`, `PROPERTIES`.
/// - Property keys in lexicographic order (provided by [PropertyExtractor]).
/// - Identifiers double-quoted only when required (reserved words, special chars).
/// - String literals always single-quoted; embedded single quotes doubled.
///
/// Stateless and thread-safe.
public final class CanonicalDdlEmitter {

  private static final String INDENT = "  ";

  private CanonicalDdlEmitter() {
  }

  /// Renders the canonical DDL for the given schema + table config.
  ///
  /// @param schema the table's schema; column declarations are derived from its field specs.
  /// @param config the table config; the table name (with type suffix stripped) and all
  /// non-default settings are emitted.
  /// @return canonical DDL ending with a semicolon and trailing newline.
  public static String emit(Schema schema, TableConfig config) {
    return emit(schema, config, null);
  }

  /// Renders the canonical DDL, scoped under `databaseName` when non-null. The database name
  /// is rendered as a leading `db.` qualifier on the table name; this matches the parser's
  /// `[db.]name` grammar.
  public static String emit(Schema schema, TableConfig config, @Nullable String databaseName) {
    rejectUnsupportedSchemaMetadata(schema);
    StringBuilder sb = new StringBuilder(512);

    String rawTableName = TableNameBuilder.extractRawTableName(config.getTableName());

    // Derive the effective database: explicit argument wins; fall back to any db. prefix already
    // embedded in the raw table name (e.g. analytics.events_OFFLINE → "analytics"). This ensures
    // the no-database overload emit(schema, config) preserves a db-qualified table name rather
    // than silently stripping it to a bare name that would land in the wrong database on replay.
    String effectiveDb = databaseName;
    if ((effectiveDb == null || effectiveDb.isEmpty()) && rawTableName.contains(".")) {
      int dot = rawTableName.indexOf('.');
      effectiveDb = rawTableName.substring(0, dot);
    }
    String displayName = rawTableName.contains(".") ? rawTableName.substring(rawTableName.indexOf('.') + 1)
        : rawTableName;

    sb.append("CREATE TABLE ");
    if (effectiveDb != null && !effectiveDb.isEmpty()) {
      sb.append(SqlIdentifiers.quote(effectiveDb)).append('.');
    }
    sb.append(SqlIdentifiers.quote(displayName));
    sb.append(" (\n");
    List<String> columns = SchemaEmitter.emitColumns(schema);
    for (int i = 0; i < columns.size(); i++) {
      sb.append(INDENT).append(columns.get(i));
      if (i < columns.size() - 1) {
        sb.append(',');
      }
      sb.append('\n');
    }
    sb.append(")\n");
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
      sb.append("PRIMARY KEY (");
      for (int i = 0; i < primaryKeyColumns.size(); i++) {
        sb.append(SqlIdentifiers.quote(primaryKeyColumns.get(i)));
        if (i < primaryKeyColumns.size() - 1) {
          sb.append(", ");
        }
      }
      sb.append(")\n");
    }

    sb.append("TABLE_TYPE = ").append(emitTableType(config.getTableType())).append('\n');

    Map<String, String> props = PropertyExtractor.extract(config);
    if (!props.isEmpty()) {
      sb.append("PROPERTIES (\n");
      int idx = 0;
      int last = props.size() - 1;
      for (Map.Entry<String, String> e : props.entrySet()) {
        sb.append(INDENT)
            .append(SqlIdentifiers.quoteString(e.getKey()))
            .append(" = ")
            .append(SqlIdentifiers.quoteString(e.getValue()));
        if (idx++ < last) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append(")");
    } else {
      // Trim the trailing newline before the semicolon when there's no PROPERTIES block.
      sb.setLength(sb.length() - 1);
    }
    sb.append(";\n");
    return sb.toString();
  }

  private static void rejectUnsupportedSchemaMetadata(Schema schema) {
    if (schema.getDescription() != null && !schema.getDescription().isEmpty()) {
      rejectUnsupportedSchemaField("description");
    }
    List<String> tags = schema.getTags();
    if (tags != null && !tags.isEmpty()) {
      rejectUnsupportedSchemaField("tags");
    }
    if (schema.isEnableColumnBasedNullHandling()) {
      rejectUnsupportedSchemaField("enableColumnBasedNullHandling");
    }
  }

  private static void rejectUnsupportedSchemaField(String field) {
    throw new IllegalArgumentException("SHOW CREATE TABLE cannot emit schema field '" + field
        + "' in canonical DDL yet; replaying the emitted DDL would silently drop that setting. "
        + "Use the JSON schema API for this table until the DDL grammar supports it.");
  }

  private static String emitTableType(TableType tableType) {
    // Exhaustive switch so a future TableType (e.g. UNIFIED) is rejected at the emit boundary
    // rather than silently rendered as REALTIME. The throw maps to the 500 path the controller
    // resource already handles for RuntimeException from emit().
    switch (tableType) {
      case OFFLINE:
        return "OFFLINE";
      case REALTIME:
        return "REALTIME";
      default:
        throw new IllegalArgumentException("Unsupported TableType for DDL emission: " + tableType);
    }
  }
}
