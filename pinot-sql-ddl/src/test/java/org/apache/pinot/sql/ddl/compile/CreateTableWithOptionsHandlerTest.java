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
package org.apache.pinot.sql.ddl.compile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the [CreateTableWithOptionsHandler] extension point: the default handler rejects the
/// options-defined `CREATE TABLE ... WITH (...)` form with actionable guidance, while a
/// registered handler receives the parsed name / flags / options and owns the compilation.
public class CreateTableWithOptionsHandlerTest {

  @AfterMethod
  public void restoreDefaultHandler() {
    /// The handler is process-wide; restore the default so sibling tests see rejecting behavior.
    DdlCompiler.setCreateTableWithOptionsHandler(new DefaultCreateTableWithOptionsHandler());
  }

  @Test
  public void defaultsToRejectingHandler() {
    assertTrue(DdlCompiler.getCreateTableWithOptionsHandler() instanceof DefaultCreateTableWithOptionsHandler);
  }

  @Test
  public void nullHandlerInstallationFailsFast() {
    // The getters are documented never-null; a null installation must fail at the setter rather
    // than as a confusing NPE on the next DDL compilation.
    expectThrows(NullPointerException.class,
        () -> DdlCompiler.setCreateTableWithOptionsHandler(null));
    expectThrows(NullPointerException.class,
        () -> DdlCompiler.setMaterializedViewDdlHandler(null));
    // The previously installed handlers survive the rejected installation.
    assertTrue(DdlCompiler.getCreateTableWithOptionsHandler() instanceof DefaultCreateTableWithOptionsHandler);
  }

  @Test
  public void defaultHandlerRejectsWithGuidance() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("CREATE TABLE trips WITH (type = 'iceberg')", DdlCompileContext.STATELESS));
    assertTrue(e.getMessage().contains("not supported"),
        "Expected 'not supported' in: " + e.getMessage());
    assertTrue(e.getMessage().contains("column-list form"),
        "Expected guidance toward the column-list form in: " + e.getMessage());
  }

  @Test
  public void registeredHandlerReceivesParsedStatement() {
    RecordingHandler handler = new RecordingHandler();
    DdlCompiler.setCreateTableWithOptionsHandler(handler);

    CompiledDdl compiled = DdlCompiler.compile(
        "CREATE TABLE IF NOT EXISTS myDb.trips WITH ("
            + "  type = 'iceberg',"
            + "  catalog_type = 'rest',"
            + "  storage.region = 'us-west-2',"
            + "  enable_schema_evolution = true"
            + ")",
        DdlCompileContext.STATELESS);

    assertEquals(handler._databaseName, "myDb");
    assertEquals(handler._tableName, "trips");
    assertTrue(handler._ifNotExists);
    // Options arrive in declaration order with case-preserved keys and stringified values.
    assertEquals(new ArrayList<>(handler._options.keySet()),
        List.of("type", "catalog_type", "storage.region", "enable_schema_evolution"));
    assertEquals(handler._options.get("type"), "iceberg");
    assertEquals(handler._options.get("enable_schema_evolution"), "true");

    assertSame(compiled, handler._result);
    assertEquals(compiled.getOperation(), DdlOperation.CREATE_TABLE);
  }

  @Test
  public void unqualifiedNameYieldsNullDatabase() {
    RecordingHandler handler = new RecordingHandler();
    DdlCompiler.setCreateTableWithOptionsHandler(handler);
    DdlCompiler.compile("CREATE TABLE trips WITH (type = 'iceberg')", DdlCompileContext.STATELESS);
    assertNull(handler._databaseName);
    assertEquals(handler._tableName, "trips");
    assertFalse(handler._ifNotExists);
  }

  @Test
  public void emptyOptionsRejectedBeforeHandler() {
    RecordingHandler handler = new RecordingHandler();
    DdlCompiler.setCreateTableWithOptionsHandler(handler);
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("CREATE TABLE trips WITH ()", DdlCompileContext.STATELESS));
    assertTrue(e.getMessage().contains("at least one option"),
        "Expected empty-options rejection in: " + e.getMessage());
    assertNull(handler._tableName, "Handler must not be invoked for empty options");
  }

  @Test
  public void duplicateOptionKeysRejectedBeforeHandler() {
    RecordingHandler handler = new RecordingHandler();
    DdlCompiler.setCreateTableWithOptionsHandler(handler);
    // Duplicate detection is case-insensitive, matching PROPERTIES behavior.
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("CREATE TABLE trips WITH (type = 'iceberg', 'TYPE' = 'delta')",
            DdlCompileContext.STATELESS));
    assertTrue(e.getMessage().contains("Duplicate property key"),
        "Expected duplicate-key rejection in: " + e.getMessage());
    assertNull(handler._tableName, "Handler must not be invoked for duplicate options");
  }

  @Test
  public void nullHandlerResultRejected() {
    DdlCompiler.setCreateTableWithOptionsHandler(
        (databaseName, tableName, ifNotExists, options, ctx) -> null);
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("CREATE TABLE trips WITH (type = 'iceberg')", DdlCompileContext.STATELESS));
    assertTrue(e.getMessage().contains("returned no result"),
        "Expected null-result rejection in: " + e.getMessage());
  }

  @Test
  public void columnListFormBypassesHandler() {
    RecordingHandler handler = new RecordingHandler();
    DdlCompiler.setCreateTableWithOptionsHandler(handler);
    CompiledDdl compiled = DdlCompiler.compile(
        "CREATE TABLE events (id INT, name STRING) TABLE_TYPE = OFFLINE", DdlCompileContext.STATELESS);
    assertEquals(compiled.getOperation(), DdlOperation.CREATE_TABLE);
    assertNull(handler._tableName, "Column-list form must not route through the options handler");
  }

  /// Records the arguments it was called with and returns a minimal valid compilation result.
  private static final class RecordingHandler implements CreateTableWithOptionsHandler {
    @Nullable
    private String _databaseName;
    @Nullable
    private String _tableName;
    private boolean _ifNotExists;
    private Map<String, String> _options = new LinkedHashMap<>();
    @Nullable
    private CompiledCreateTable _result;

    @Override
    public CompiledCreateTable compile(@Nullable String databaseName, String tableName, boolean ifNotExists,
        Map<String, String> options, DdlCompileContext ctx) {
      _databaseName = databaseName;
      _tableName = tableName;
      _ifNotExists = ifNotExists;
      _options = options;
      Schema schema = new Schema();
      schema.setSchemaName(tableName);
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
      _result = new CompiledCreateTable(databaseName, schema, tableConfig, ifNotExists, new ArrayList<>());
      return _result;
    }
  }
}
