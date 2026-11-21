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
import org.apache.pinot.spi.config.table.TableType;


/// Immutable, thread-safe compile result for `CREATE TABLE target LIKE source [TYPE OFFLINE | REALTIME]`.
/// Source and destination databases remain independent; the controller resolves their request defaults and
/// authorizes both references before looking up and copying the source definition.
public final class CompiledCreateTableLike extends CompiledDdl {
  private final String _rawTableName;
  @Nullable
  private final String _sourceDatabaseName;
  private final String _sourceRawTableName;
  @Nullable
  private final TableType _tableType;
  private final boolean _ifNotExists;

  public CompiledCreateTableLike(@Nullable String databaseName, String rawTableName,
      @Nullable String sourceDatabaseName, String sourceRawTableName, @Nullable TableType tableType,
      boolean ifNotExists) {
    super(DdlOperation.CREATE_TABLE_LIKE, databaseName, List.of());
    _rawTableName = rawTableName;
    _sourceDatabaseName = sourceDatabaseName;
    _sourceRawTableName = sourceRawTableName;
    _tableType = tableType;
    _ifNotExists = ifNotExists;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  @Nullable
  public String getSourceDatabaseName() {
    return _sourceDatabaseName;
  }

  public String getSourceRawTableName() {
    return _sourceRawTableName;
  }

  /// Returns the selected source variant, or `null` to copy all existing variants, including a hybrid pair.
  @Nullable
  public TableType getTableType() {
    return _tableType;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }
}
