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
package org.apache.pinot.segment.local.columntransformer;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for TimeValidationColumnTransformer.
 */
public class TimeValidationColumnTransformerTest {

  @Test
  public void testIsNoOpWhenNoTimeColumn() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.INT)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);
    assertTrue(transformer.isNoOp(), "Should be no-op when no time column configured");
  }

  @Test
  public void testIsNoOpWhenTimeValueCheckDisabled() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .build();
    // rowTimeValueCheck is false by default

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);
    assertTrue(transformer.isNoOp(), "Should be no-op when time value check is disabled");
  }

  @Test
  public void testIsNoOpWhenTimeValueCheckEnabled() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);
    assertTrue(!transformer.isNoOp(), "Should not be no-op when time value check is enabled");
  }

  @Test
  public void testTransformValidTime() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);

    // Valid time: current time
    long validTime = System.currentTimeMillis();
    Object result = transformer.transform(validTime);

    assertEquals(result, validTime, "Valid time should pass through unchanged");
  }

  @Test
  public void testTransformNull() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);
    Object result = transformer.transform(null);

    assertNull(result, "Null should return null");
  }

  @Test
  public void testTransformInvalidTimeWithContinueOnError() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);
    ingestionConfig.setContinueOnError(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);

    // Invalid time: year 2100 (after 2071)
    long invalidTime = 4102444800000L;
    Object result = transformer.transform(invalidTime);

    assertNull(result, "Invalid time should be transformed to null with continueOnError");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTransformInvalidTimeWithoutContinueOnError() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);
    ingestionConfig.setContinueOnError(false);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);

    // Invalid time: year 2100 (after 2071)
    long invalidTime = 4102444800000L;
    transformer.transform(invalidTime); // Should throw
  }

  @Test
  public void testTransformStringTime() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);

    // Valid time as string
    String validTime = String.valueOf(System.currentTimeMillis());
    Object result = transformer.transform(validTime);

    assertEquals(result, validTime, "Valid time string should pass through unchanged");
  }

  @Test
  public void testTransformTimeAtValidBoundary() {
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .setIngestionConfig(ingestionConfig)
        .build();

    TimeValidationColumnTransformer transformer = new TimeValidationColumnTransformer(tableConfig, schema);

    // Time at the start of valid range (1971)
    long boundaryTime = 31536000000L; // Jan 1, 1971 in millis
    Object result = transformer.transform(boundaryTime);

    assertEquals(result, boundaryTime, "Time at 1971 boundary should be valid");
  }
}
