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


/**
 * Renders a {@code CREATE TABLE} statement in canonical Pinot DDL form from a {@link Schema} and
 * {@link TableConfig}. Designed so that {@code parse(emit(schema, config))} round-trips back to
 * a semantically-equivalent (Schema, TableConfig) pair.
 *
 * <p>Canonical formatting rules:
 * <ul>
 *   <li>Two-space indentation, one column per line, trailing comma between entries.</li>
 *   <li>Clause order: column block, {@code TABLE_TYPE}, {@code PROPERTIES}.</li>
 *   <li>Property keys in lexicographic order (provided by {@link PropertyExtractor}).</li>
 *   <li>Identifiers double-quoted only when required (reserved words, special chars).</li>
 *   <li>String literals always single-quoted; embedded single quotes doubled.</li>
 * </ul>
 *
 * <p>Stateless and thread-safe.
 */
public final class CanonicalDdlEmitter {

  private static final String INDENT = "  ";

  private CanonicalDdlEmitter() {
  }

  /**
   * Renders the canonical DDL for the given schema + table config.
   *
   * @param schema the table's schema; column declarations are derived from its field specs.
   * @param config the table config; the table name (with type suffix stripped) and all
   *     non-default settings are emitted.
   * @return canonical DDL ending with a semicolon and trailing newline.
   */
  public static String emit(Schema schema, TableConfig config) {
    return emit(schema, config, null);
  }

  /**
   * Renders the canonical DDL, scoped under {@code databaseName} when non-null. The database name
   * is rendered as a leading {@code db.} qualifier on the table name; this matches the parser's
   * {@code [db.]name} grammar.
   */
  public static String emit(Schema schema, TableConfig config, @Nullable String databaseName) {
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

  private static String emitTableType(TableType tableType) {
    return tableType == TableType.OFFLINE ? "OFFLINE" : "REALTIME";
  }
}
