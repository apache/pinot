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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.UploadedRealtimeSegmentNameGenerator;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexPlugin;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


// TODO: add more tests here.
public class SegmentGeneratorConfigTest {
  private IndexService _originalIndexService;

  @BeforeClass
  public void setUpIndexService() {
    _originalIndexService = IndexService.getInstance();
    IndexType<?, ?, ?> dictionaryIndex = new DummyDictionaryIndexType(StandardIndexes.DICTIONARY_ID);
    IndexType<?, ?, ?> forwardIndex = new DummyForwardIndexType(StandardIndexes.FORWARD_ID);
    IndexService.setInstance(
        new IndexService(Set.of(new DummyIndexPlugin(dictionaryIndex), new DummyIndexPlugin(forwardIndex))));
  }

  @AfterClass
  public void tearDownIndexService() {
    IndexService.setInstance(_originalIndexService);
  }

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

    // Table config is externally partitioned
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("test").build();

    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setUploadedSegmentPartitionId(0);
    segmentGeneratorConfig.setCreationTime("1234567890");
    segmentGeneratorConfig.setSegmentNamePrefix("prefix");
    segmentGeneratorConfig.setSegmentNamePostfix("5");

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof UploadedRealtimeSegmentNameGenerator);
    Assert.assertTrue(
        segmentGeneratorConfig.getSegmentNameGenerator().toString().contains("tableName=test"));

    // Table config has no time column defined
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);

    Assert.assertTrue(segmentGeneratorConfig.getSegmentNameGenerator() instanceof SimpleSegmentNameGenerator);
  }

  private static class DummyIndexPlugin implements IndexPlugin<IndexType<?, ?, ?>> {
    private final IndexType<?, ?, ?> _indexType;

    DummyIndexPlugin(IndexType<?, ?, ?> indexType) {
      _indexType = indexType;
    }

    @Override
    public IndexType<?, ?, ?> getIndexType() {
      return _indexType;
    }
  }

  private static class DummyDictionaryIndexType extends
      AbstractIndexType<org.apache.pinot.segment.spi.index.DictionaryIndexConfig, IndexReader, IndexCreator> {
    DummyDictionaryIndexType(String id) {
      super(id);
    }

    @Override
    public Class<org.apache.pinot.segment.spi.index.DictionaryIndexConfig> getIndexConfigClass() {
      return org.apache.pinot.segment.spi.index.DictionaryIndexConfig.class;
    }

    @Override
    public org.apache.pinot.segment.spi.index.DictionaryIndexConfig getDefaultConfig() {
      return org.apache.pinot.segment.spi.index.DictionaryIndexConfig.DEFAULT;
    }

    @Override
    public IndexCreator createIndexCreator(org.apache.pinot.segment.spi.creator.IndexCreationContext context,
        org.apache.pinot.segment.spi.index.DictionaryIndexConfig indexConfig) {
      return null;
    }

    @Override
    protected IndexReaderFactory<IndexReader> createReaderFactory() {
      return (segmentReader, fieldIndexConfigs, metadata) -> null;
    }

    @Override
    public java.util.List<String> getFileExtensions(org.apache.pinot.segment.spi.ColumnMetadata columnMetadata) {
      return java.util.List.of();
    }

    @Override
    public org.apache.pinot.segment.spi.index.IndexHandler createIndexHandler(
        org.apache.pinot.segment.spi.store.SegmentDirectory segmentDirectory,
        java.util.Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      return null;
    }
  }

  private static class DummyForwardIndexType extends
      AbstractIndexType<org.apache.pinot.segment.spi.index.ForwardIndexConfig, IndexReader, IndexCreator> {
    DummyForwardIndexType(String id) {
      super(id);
    }

    @Override
    public Class<org.apache.pinot.segment.spi.index.ForwardIndexConfig> getIndexConfigClass() {
      return org.apache.pinot.segment.spi.index.ForwardIndexConfig.class;
    }

    @Override
    public org.apache.pinot.segment.spi.index.ForwardIndexConfig getDefaultConfig() {
      return org.apache.pinot.segment.spi.index.ForwardIndexConfig.getDefault();
    }

    @Override
    public IndexCreator createIndexCreator(org.apache.pinot.segment.spi.creator.IndexCreationContext context,
        org.apache.pinot.segment.spi.index.ForwardIndexConfig indexConfig) {
      return null;
    }

    @Override
    protected IndexReaderFactory<IndexReader> createReaderFactory() {
      return (segmentReader, fieldIndexConfigs, metadata) -> null;
    }

    @Override
    public java.util.List<String> getFileExtensions(org.apache.pinot.segment.spi.ColumnMetadata columnMetadata) {
      return java.util.List.of();
    }

    @Override
    public org.apache.pinot.segment.spi.index.IndexHandler createIndexHandler(
        org.apache.pinot.segment.spi.store.SegmentDirectory segmentDirectory,
        java.util.Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      return null;
    }
  }
}
