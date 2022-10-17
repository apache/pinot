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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
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

  @Test
  public void testExtractTimestampIndexConfigsFromTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t1").setFieldConfigList(
        ImmutableList.of(new FieldConfig("f1", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.ZSTANDARD,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.DAY)), ImmutableMap.of()),
            new FieldConfig("f2", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.LZ4,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.WEEK)), ImmutableMap.of()),
            new FieldConfig("f3", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.PASS_THROUGH,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.MONTH)), ImmutableMap.of()))).build();
    Map<String, List<TimestampIndexGranularity>> timestampIndexGranularityMap =
        SegmentGeneratorConfig.extractTimestampIndexConfigsFromTableConfig(tableConfig);
    Assert.assertEquals(3, timestampIndexGranularityMap.size());
    Assert.assertTrue(timestampIndexGranularityMap.containsKey("f1"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f1").contains(TimestampIndexGranularity.DAY));
    Assert.assertTrue(timestampIndexGranularityMap.containsKey("f2"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f2").contains(TimestampIndexGranularity.WEEK));
    Assert.assertTrue(timestampIndexGranularityMap.containsKey("f3"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f3").contains(TimestampIndexGranularity.MONTH));
  }

  @Test
  public void testUpdateSchemaWithTimestampIndexes() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t1").setFieldConfigList(
        ImmutableList.of(new FieldConfig("f1", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.ZSTANDARD,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.DAY)), ImmutableMap.of()),
            new FieldConfig("f2", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.LZ4,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.WEEK)), ImmutableMap.of()),
            new FieldConfig("f3", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.PASS_THROUGH,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.MONTH)), ImmutableMap.of()))).build();
    Map<String, List<TimestampIndexGranularity>> timestampIndexGranularityMap =
        SegmentGeneratorConfig.extractTimestampIndexConfigsFromTableConfig(tableConfig);
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("f1", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:SECONDS")
        .addDateTime("f2", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:SECONDS")
        .addDateTime("f3", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:SECONDS")
        .build();

    // No update when no timestampIndexGranularityMap
    Assert.assertEquals(schema, SegmentGeneratorConfig.updateSchemaWithTimestampIndexes(schema, ImmutableMap.of()));

    Schema updatedSchema =
        SegmentGeneratorConfig.updateSchemaWithTimestampIndexes(schema, timestampIndexGranularityMap);
    Assert.assertEquals(6, updatedSchema.getAllFieldSpecs().size());
    Assert.assertTrue(updatedSchema.hasColumn("$f1$DAY"));
    Assert.assertTrue(updatedSchema.hasColumn("$f2$WEEK"));
    Assert.assertTrue(updatedSchema.hasColumn("$f3$MONTH"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f1").contains(TimestampIndexGranularity.DAY));
    Assert.assertTrue(timestampIndexGranularityMap.containsKey("f2"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f2").contains(TimestampIndexGranularity.WEEK));
    Assert.assertTrue(timestampIndexGranularityMap.containsKey("f3"));
    Assert.assertTrue(timestampIndexGranularityMap.get("f3").contains(TimestampIndexGranularity.MONTH));
  }

  @Test
  public void inferNameGeneratorType() {
    // Time column is data type STRING and in SDF
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("date").build();
    Schema schema = new Schema.SchemaBuilder().addDateTime("date", FieldSpec.DataType.STRING,
        "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS").build();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof NormalizedDateSegmentNameGenerator);

    // Use name generator if defined
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setSegmentNameGenerator(new FixedSegmentNameGenerator("test"));

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof FixedSegmentNameGenerator);

    // Use fixed segment generator if segment name defined
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setSegmentName("test");

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof FixedSegmentNameGenerator);

    // Default to simple name generator with prefix when time column is not in SDF
    schema = new Schema.SchemaBuilder().addDateTime("date", FieldSpec.DataType.STRING,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setSegmentNamePrefix("prefix");

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof SimpleSegmentNameGenerator);
    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator().toString().contains("tableName=prefix"));

    // Default to simple name generator with table prefix
    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof SimpleSegmentNameGenerator);
    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator().toString().contains("tableName=testTable"));

    // Table config has no time column defined
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof SimpleSegmentNameGenerator);
  }
}
