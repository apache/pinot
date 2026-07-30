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
package org.apache.pinot.materializedview.handler;

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.PinotQuery;


/// Inputs the broker passes to [MaterializedViewHandler#compile] at query compile time.
///
/// Carries the post-base-rewrite server query (after standard broker rewrites such as HLL log2m
/// and approximate-function override have run) plus the table names and table cache the handler
/// needs to (a) decide whether an MV applies and (b) construct any rewritten server query /
/// split context.  The MV-side schema is fetched on demand via [#getTableCache()]; the base
/// table schema is reachable the same way if a custom handler ever needs it.
public final class MaterializedViewCompileContext {
  private final PinotQuery _serverPinotQuery;
  private final String _tableNameWithType;
  private final String _rawTableName;
  private final TableCache _tableCache;

  public MaterializedViewCompileContext(PinotQuery serverPinotQuery, String tableNameWithType,
      String rawTableName, TableCache tableCache) {
    _serverPinotQuery = serverPinotQuery;
    _tableNameWithType = tableNameWithType;
    _rawTableName = rawTableName;
    _tableCache = tableCache;
  }

  public PinotQuery getServerPinotQuery() {
    return _serverPinotQuery;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public TableCache getTableCache() {
    return _tableCache;
  }
}
