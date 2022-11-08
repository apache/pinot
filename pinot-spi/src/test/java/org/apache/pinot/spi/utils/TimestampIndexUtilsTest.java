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
package org.apache.pinot.spi.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TimestampIndexUtilsTest {

  @Test
  public void testTimestampIndexGranularity() {
    assertTrue(TimestampIndexUtils.isValidGranularity("DAY"));
    assertFalse(TimestampIndexUtils.isValidGranularity("day"));

    String timestampColumn = "testTs";
    TimestampIndexGranularity granularity = TimestampIndexGranularity.DAY;
    assertEquals(TimestampIndexUtils.getColumnWithGranularity(timestampColumn, granularity), "$testTs$DAY");
    assertTrue(TimestampIndexUtils.isValidColumnWithGranularity("$testTs$DAY"));
    assertFalse(TimestampIndexUtils.isValidColumnWithGranularity(timestampColumn));
    assertFalse(TimestampIndexUtils.isValidColumnWithGranularity("$docId"));
    assertFalse(TimestampIndexUtils.isValidColumnWithGranularity("$ts$"));
  }

  @Test
  public void testExtractColumnsWithGranularity() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setFieldConfigList(
        Arrays.asList(
            new FieldConfig("ts1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TIMESTAMP, null, null,
                new TimestampConfig(Arrays.asList(TimestampIndexGranularity.SECOND, TimestampIndexGranularity.MINUTE,
                    TimestampIndexGranularity.HOUR)), null),
            new FieldConfig("ts2", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TIMESTAMP, null, null,
                new TimestampConfig(Arrays.asList(TimestampIndexGranularity.HOUR, TimestampIndexGranularity.DAY,
                    TimestampIndexGranularity.WEEK)), null),
            new FieldConfig("ts3", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TIMESTAMP, null,
                FieldConfig.CompressionCodec.PASS_THROUGH, new TimestampConfig(
                Arrays.asList(TimestampIndexGranularity.WEEK, TimestampIndexGranularity.MONTH,
                    TimestampIndexGranularity.YEAR)), null))).build();
    Set<String> columnsWithGranularity = TimestampIndexUtils.extractColumnsWithGranularity(tableConfig);
    assertEquals(columnsWithGranularity, new HashSet<>(
        Arrays.asList("$ts1$SECOND", "$ts1$MINUTE", "$ts1$HOUR", "$ts2$HOUR", "$ts2$DAY", "$ts2$WEEK", "$ts3$WEEK",
            "$ts3$MONTH", "$ts3$YEAR")));
  }

  @Test
  public void testApplyTimestampIndex() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setFieldConfigList(
        Arrays.asList(
            new FieldConfig("ts1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TIMESTAMP, null, null,
                new TimestampConfig(Arrays.asList(TimestampIndexGranularity.SECOND, TimestampIndexGranularity.MINUTE,
                    TimestampIndexGranularity.HOUR)), null),
            new FieldConfig("ts2", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TIMESTAMP, null, null,
                new TimestampConfig(Arrays.asList(TimestampIndexGranularity.DAY, TimestampIndexGranularity.WEEK,
                    TimestampIndexGranularity.MONTH)), null))).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("ts1", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS")
        .addDateTime("ts2", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();

    // Apply TIMESTAMP index multiple times should get the same result
    for (int i = 0; i < 5; i++) {
      TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);

      // Check schema
      assertEquals(schema.size(), 8);
      FieldSpec ts1SecondFieldSpec = schema.getFieldSpecFor("$ts1$SECOND");
      assertNotNull(ts1SecondFieldSpec);
      assertTrue(ts1SecondFieldSpec instanceof DateTimeFieldSpec);
      DateTimeFieldSpec ts1SecondDateTimeFieldSpec = (DateTimeFieldSpec) ts1SecondFieldSpec;
      assertEquals(ts1SecondDateTimeFieldSpec.getDataType(), DataType.TIMESTAMP);
      assertEquals(ts1SecondDateTimeFieldSpec.getFormat(), "TIMESTAMP");
      assertEquals(ts1SecondDateTimeFieldSpec.getGranularity(), "1:SECONDS");

      // Check ingestion transform
      assertNotNull(tableConfig.getIngestionConfig());
      List<TransformConfig> transformConfigs = tableConfig.getIngestionConfig().getTransformConfigs();
      assertNotNull(transformConfigs);
      assertEquals(transformConfigs.size(), 6);
      Set<String> transformColumns = new HashSet<>();
      for (TransformConfig transformConfig : transformConfigs) {
        String columnName = transformConfig.getColumnName();
        assertTrue(transformColumns.add(columnName));
        if (columnName.equals("$ts2$DAY")) {
          assertEquals(transformConfig.getTransformFunction(), "dateTrunc('DAY',\"ts2\")");
        }
      }
      assertEquals(transformColumns, new HashSet<>(
          Arrays.asList("$ts1$SECOND", "$ts1$MINUTE", "$ts1$HOUR", "$ts2$DAY", "$ts2$WEEK", "$ts2$MONTH")));

      // Check range index
      List<String> rangeIndexColumns = tableConfig.getIndexingConfig().getRangeIndexColumns();
      assertNotNull(rangeIndexColumns);
      assertEquals(new HashSet<>(rangeIndexColumns), transformColumns);
    }
  }
}
