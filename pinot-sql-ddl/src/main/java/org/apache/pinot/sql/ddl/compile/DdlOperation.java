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

/// Discriminator for the operation a [CompiledDdl] represents.
///
/// Ordering convention (mirrored by `DdlCompiler#compile`, `PinotDdlRestletResource#executeDdl`,
/// and §3/§4 of `DESIGN.md`): Catalog → Table → Materialized View. Within each object-level
/// family (Table, Materialized View) the lifecycle order CREATE → SHOW CREATE → DROP is used,
/// so the two families read as mirror images of each other — making it obvious at a glance that
/// every Table verb has a Materialized View counterpart (the "Q2=B" strict-type-partitioning
/// contract). Catalog-level operations (multi-object reads like SHOW TABLES and
/// SHOW MATERIALIZED VIEWS) are grouped first because they have no object lifecycle and are
/// typically the first call a user makes against a fresh database. Future schema-level verbs
/// (e.g. SHOW DATABASES, SHOW FUNCTIONS) belong in the Catalog group, not in the object-level
/// Table/MV groups.
public enum DdlOperation {
  // Catalog (schema-level, multi-object reads). Listed in object-family order to mirror the
  // Table → Materialized View grouping of the object-level lifecycle verbs below.
  SHOW_TABLES,
  SHOW_MATERIALIZED_VIEWS,

  // Table (single-object lifecycle).
  CREATE_TABLE,
  SHOW_CREATE_TABLE,
  DROP_TABLE,

  // Materialized View (single-object lifecycle, mirrors Table).
  CREATE_MATERIALIZED_VIEW,
  SHOW_CREATE_MATERIALIZED_VIEW,
  DROP_MATERIALIZED_VIEW
}
