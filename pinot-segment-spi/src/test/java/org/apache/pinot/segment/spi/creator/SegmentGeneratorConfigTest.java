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
package org.apache.pinot.segment.spi.creator;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


// TODO: add more tests here.
public class SegmentGeneratorConfigTest {

  @Test
  public void testEpochTime() {
    Schema schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName("daysSinceEpoch").build();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "daysSinceEpoch");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.EPOCH);
    assertEquals(segmentGeneratorConfig.getSegmentTimeUnit(), TimeUnit.DAYS);
    assertNotNull(segmentGeneratorConfig.getDateTimeFormatSpec());

    // MUST provide valid tableConfig with time column if time details are wanted
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertNull(segmentGeneratorConfig.getTimeColumnName());
    assertNull(segmentGeneratorConfig.getSegmentTimeUnit());
    assertNull(segmentGeneratorConfig.getDateTimeFormatSpec());

    schema = new Schema.SchemaBuilder().addDateTime("daysSinceEpoch", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName("daysSinceEpoch").build();
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "daysSinceEpoch");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.EPOCH);
    assertEquals(segmentGeneratorConfig.getSegmentTimeUnit(), TimeUnit.DAYS);
    assertNotNull(segmentGeneratorConfig.getDateTimeFormatSpec());
  }

  @Test
  public void testSimpleDateFormat() {
    Schema schema = new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(FieldSpec.DataType.STRING, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd", "Date"), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName("Date").build();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "Date");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE);
    assertNull(segmentGeneratorConfig.getSegmentTimeUnit());
    assertNotNull(segmentGeneratorConfig.getDateTimeFormatSpec());
    assertEquals(segmentGeneratorConfig.getDateTimeFormatSpec().getSDFPattern(), "yyyyMMdd");

    // MUST provide valid tableConfig with time column if time details are wanted
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertNull(segmentGeneratorConfig.getTimeColumnName());
    assertNull(segmentGeneratorConfig.getSegmentTimeUnit());
    assertNull(segmentGeneratorConfig.getDateTimeFormatSpec());

    schema = new Schema.SchemaBuilder()
        .addDateTime("Date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS").build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName("Date").build();
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "Date");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE);
    assertNull(segmentGeneratorConfig.getSegmentTimeUnit());
    assertNotNull(segmentGeneratorConfig.getDateTimeFormatSpec());
    assertEquals(segmentGeneratorConfig.getDateTimeFormatSpec().getSDFPattern(), "yyyyMMdd");
  }
}
