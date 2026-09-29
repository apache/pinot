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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Result of compiling `CREATE TABLE ...`.
public final class CompiledCreateTable extends CompiledDdl {
  private final Schema _schema;
  private final TableConfig _tableConfig;
  private final boolean _ifNotExists;

  public CompiledCreateTable(@Nullable String databaseName, Schema schema, TableConfig tableConfig,
      boolean ifNotExists, List<String> warnings) {
    super(DdlOperation.CREATE_TABLE, databaseName, warnings);
    _schema = schema;
    _tableConfig = tableConfig;
    _ifNotExists = ifNotExists;
  }

  public Schema getSchema() {
    return _schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }
}
