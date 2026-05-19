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

import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableType;


/// Result of compiling `SHOW CREATE TABLE [db.]name [TYPE OFFLINE | REALTIME]`.
///
/// This is a lookup-only compile result: it carries the target identifier and (optional)
/// type filter; the controller is responsible for fetching the persisted Schema + TableConfig and
/// running them through the canonical DDL emitter.
public final class CompiledShowCreateTable extends CompiledDdl {
  private final String _rawTableName;
  private final TableType _tableType;

  public CompiledShowCreateTable(@Nullable String databaseName, String rawTableName,
      @Nullable TableType tableType) {
    super(DdlOperation.SHOW_CREATE_TABLE, databaseName, Collections.emptyList());
    _rawTableName = rawTableName;
    _tableType = tableType;
  }

  /// Bare table name with no database prefix and no _OFFLINE/_REALTIME suffix.
  public String getRawTableName() {
    return _rawTableName;
  }

  /// @return the requested type, or `null` when the user omitted the `TYPE` clause.
  /// When null, the controller picks the variant that exists; when both OFFLINE and REALTIME
  /// variants exist, the controller returns 400 BAD_REQUEST and the caller must specify
  /// `TYPE OFFLINE` or `TYPE REALTIME` explicitly.
  @Nullable
  public TableType getTableType() {
    return _tableType;
  }
}
