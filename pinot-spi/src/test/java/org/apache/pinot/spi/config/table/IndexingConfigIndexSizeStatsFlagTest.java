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
package org.apache.pinot.spi.config.table;

import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Pins the wire contract of `indexSizeStatsEnabled`. The field is persisted in ZooKeeper as part of the table
/// config, so its default, its JSON property name and its behaviour on configs that predate it all have to stay
/// fixed. Mirrors [IndexingConfigCompressionFlagTest] for the sibling flag.
public class IndexingConfigIndexSizeStatsFlagTest {

  @Test
  public void testDefaultValueIsFalse() {
    IndexingConfig config = new IndexingConfig();
    assertFalse(config.isIndexSizeStatsEnabled(),
        "indexSizeStatsEnabled must default to false so index size collection is opt-in");
  }

  @Test
  public void testSetAndGet() {
    IndexingConfig config = new IndexingConfig();
    config.setIndexSizeStatsEnabled(true);
    assertTrue(config.isIndexSizeStatsEnabled(), "indexSizeStatsEnabled should reflect the value that was set");
  }

  /// The JSON property name is part of the persisted contract; renaming it would silently disable the feature on
  /// existing tables.
  @Test
  public void testJsonSerializationRoundTrip()
      throws Exception {
    IndexingConfig original = new IndexingConfig();
    original.setIndexSizeStatsEnabled(true);

    String json = JsonUtils.objectToString(original);
    assertTrue(json.contains("\"indexSizeStatsEnabled\""),
        "Serialized config should carry the indexSizeStatsEnabled property, got: " + json);

    IndexingConfig deserialized = JsonUtils.stringToObject(json, IndexingConfig.class);
    assertTrue(deserialized.isIndexSizeStatsEnabled(), "indexSizeStatsEnabled should survive a JSON round trip");
  }

  /// Table configs written before this flag existed have no such property and must still deserialize, with the
  /// feature off.
  @Test
  public void testBackwardCompatDeserialization()
      throws Exception {
    String oldConfigJson = "{\"loadMode\":\"MMAP\",\"compressionStatsEnabled\":true}";

    IndexingConfig config = JsonUtils.stringToObject(oldConfigJson, IndexingConfig.class);

    assertFalse(config.isIndexSizeStatsEnabled(),
        "A config predating the flag must deserialize with index size stats disabled");
    assertTrue(config.isCompressionStatsEnabled(), "Unrelated existing flags must be unaffected");
  }

  /// [TableConfigBuilder] is the path most callers and tests use, so the flag has to reach [IndexingConfig] through
  /// it rather than only via the setter.
  @Test
  public void testTableConfigBuilderPropagation() {
    TableConfig enabled = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIndexSizeStatsEnabled(true)
        .build();
    assertTrue(enabled.getIndexingConfig().isIndexSizeStatsEnabled(),
        "TableConfigBuilder should propagate indexSizeStatsEnabled to the IndexingConfig");

    TableConfig defaulted = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    assertFalse(defaulted.getIndexingConfig().isIndexSizeStatsEnabled(),
        "A table config built without the flag must leave index size stats disabled");
  }
}
