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


/// Result of compiling `DROP TABLE ...`.
public final class CompiledDropTable extends CompiledDdl {
  private final String _rawTableName;
  private final TableType _tableType;
  private final boolean _ifExists;

  public CompiledDropTable(@Nullable String databaseName, String rawTableName,
      @Nullable TableType tableType, boolean ifExists) {
    super(DdlOperation.DROP_TABLE, databaseName, Collections.emptyList());
    _rawTableName = rawTableName;
    _tableType = tableType;
    _ifExists = ifExists;
  }

  /// Bare table name with no database prefix and no _OFFLINE/_REALTIME suffix.
  public String getRawTableName() {
    return _rawTableName;
  }

  /// @return the requested type to drop, or `null` when both OFFLINE and REALTIME variants
  /// should be dropped.
  @Nullable
  public TableType getTableType() {
    return _tableType;
  }

  public boolean isIfExists() {
    return _ifExists;
  }
}
