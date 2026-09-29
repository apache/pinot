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
package org.apache.pinot.segment.local.segment.creator;

import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies that `compressionStatsEnabled` propagates from [TableConfig] to [SegmentGeneratorConfig].
public class SegmentGeneratorConfigPropagationTest {

  /// Verifies explicit enablement is propagated.
  @Test
  public void testCompressionStatsEnabledPropagation() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", DataType.INT)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    assertTrue(config.isCompressionStatsEnabled(),
        "compressionStatsEnabled should be true when explicitly enabled on TableConfig");
  }

  /// Verifies the default remains disabled.
  @Test
  public void testCompressionStatsDisabledByDefault() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", DataType.INT)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    assertFalse(config.isCompressionStatsEnabled(),
        "compressionStatsEnabled should be false by default when not set on TableConfig");
  }
}
