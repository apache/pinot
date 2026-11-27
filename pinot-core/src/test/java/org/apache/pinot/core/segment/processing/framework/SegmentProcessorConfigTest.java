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
package org.apache.pinot.core.segment.processing.framework;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit test for the [SegmentProcessorConfig] build-time validations, most importantly the VARIANT merge-type
/// choke point that every merge execution path (task executors, command-line tools) flows through.
public class SegmentProcessorConfigTest {
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  private static Schema schema(boolean withVariant) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("name", FieldSpec.DataType.STRING);
    if (withVariant) {
      builder.addSingleValueDimension("payload", FieldSpec.DataType.VARIANT);
    }
    return builder.build();
  }

  @Test
  public void testVariantColumnRejectsRollupAndDedupAtTheChokePoint() {
    for (MergeType rejected : new MergeType[]{MergeType.ROLLUP, MergeType.DEDUP}) {
      IllegalStateException exception = expectThrows(IllegalStateException.class,
          () -> new SegmentProcessorConfig.Builder().setTableConfig(TABLE_CONFIG).setSchema(schema(true))
              .setMergeType(rejected).build());
      assertTrue(exception.getMessage().contains("VARIANT"), exception.getMessage());
    }

    // CONCAT (explicit and default) builds; a schema without VARIANT accepts every merge type.
    assertEquals(new SegmentProcessorConfig.Builder().setTableConfig(TABLE_CONFIG).setSchema(schema(true))
        .setMergeType(MergeType.CONCAT).build().getMergeType(), MergeType.CONCAT);
    assertEquals(new SegmentProcessorConfig.Builder().setTableConfig(TABLE_CONFIG).setSchema(schema(true))
        .build().getMergeType(), MergeType.CONCAT);
    assertEquals(new SegmentProcessorConfig.Builder().setTableConfig(TABLE_CONFIG).setSchema(schema(false))
        .setMergeType(MergeType.ROLLUP).build().getMergeType(), MergeType.ROLLUP);
  }
}
