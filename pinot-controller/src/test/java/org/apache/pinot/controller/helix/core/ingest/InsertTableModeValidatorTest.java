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
package org.apache.pinot.controller.helix.core.ingest;

import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Unit tests for {@link InsertTableModeValidator}. Locks the v1 rule set against future regressions
/// — particularly the materialized-view rejection that protects MV invariants from direct INSERT.
public class InsertTableModeValidatorTest {

  @Test
  public void testAcceptsPlainOfflineTable() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertNull(InsertTableModeValidator.validate(tc, "row"));
  }

  /// INSERT INTO must reject materialized-view tables: MV data is computed from base tables by
  /// `MaterializedViewTask`; a direct INSERT would write rows that do not exist in the
  /// source data, breaking the MV invariant. This guard prevents the gap introduced by upstream
  /// #18544 (MV DDL) from being exploitable via `controller.insert.enabled=true`.
  @Test
  public void testRejectsMaterializedView() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName("mv_t")
        .setIsMaterializedView(true).build();
    String err = InsertTableModeValidator.validate(tc, "row");
    assertNotNull(err);
    assertTrue(err.contains("Materialized view"),
        "Error must mention 'Materialized view'; got: " + err);
    /// Lock the insert-kind label more strictly than a bare "row" substring (which could match
    /// adjacent words like "throw"/"borrow" in a future error-message rewrite).
    assertTrue(err.contains("direct row insert"),
        "Error must mention 'direct row insert'; got: " + err);
  }

  @Test
  public void testRejectsDedup() {
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    TableConfig tc = new TableConfigBuilder(TableType.REALTIME).setTableName("t")
        .setDedupConfig(dedupConfig).build();
    String err = InsertTableModeValidator.validate(tc, "file");
    assertNotNull(err);
    assertTrue(err.contains("Dedup"), "Error must mention 'Dedup'; got: " + err);
  }

  @Test
  public void testRejectsPartialUpsert() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    TableConfig tc = new TableConfigBuilder(TableType.REALTIME).setTableName("t")
        .setUpsertConfig(upsertConfig).build();
    String err = InsertTableModeValidator.validate(tc, "row");
    assertNotNull(err);
    assertTrue(err.contains("Partial upsert"),
        "Error must mention 'Partial upsert'; got: " + err);
  }

  @Test
  public void testRejectsFullUpsertWithoutPartitionConfig() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tc = new TableConfigBuilder(TableType.REALTIME).setTableName("t")
        .setUpsertConfig(upsertConfig).build();
    String err = InsertTableModeValidator.validate(tc, "row");
    assertNotNull(err);
    assertTrue(err.contains("Full upsert"),
        "Error must mention 'Full upsert'; got: " + err);
  }
}
