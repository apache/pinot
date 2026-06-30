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

import java.util.Map;
import javax.annotation.Nullable;


/// Default [CreateTableWithOptionsHandler]. OSS Pinot has no built-in options-defined table
/// type, so the `CREATE TABLE ... WITH (options)` form is rejected with guidance toward the
/// column-list form. A distribution that supports options-defined tables (e.g. external tables
/// backed by an Iceberg catalog) replaces this handler via
/// [DdlCompiler#setCreateTableWithOptionsHandler].
///
/// Stateless and thread-safe.
public class DefaultCreateTableWithOptionsHandler implements CreateTableWithOptionsHandler {

  @Override
  public CompiledCreateTable compile(@Nullable String databaseName, String tableName, boolean ifNotExists,
      Map<String, String> options, DdlCompileContext ctx) {
    throw new DdlCompilationException(
        "CREATE TABLE ... WITH (options) is not supported: no options-defined table handler is "
            + "installed in this distribution. Use the column-list form instead: CREATE TABLE "
            + tableName + " (col TYPE, ...) TABLE_TYPE = OFFLINE | REALTIME [PROPERTIES (...)].");
  }
}
