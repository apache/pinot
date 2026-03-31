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
package org.apache.pinot.segment.spi.index;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FieldIndexConfigsUtilTest {
  private IndexService _originalIndexService;

  @BeforeClass
  public void setUpIndexService() {
    _originalIndexService = IndexService.getInstance();
    IndexService.setInstance(new IndexService(Set.of(new TestIndexPlugin(new TestDictionaryIndexType()),
        new TestIndexPlugin(new TestForwardIndexType()), new TestIndexPlugin(new TestInvertedIndexType()))));
  }

  @AfterClass(alwaysRun = true)
  public void tearDownIndexService() {
    IndexService.setInstance(_originalIndexService);
  }

  @Test
  public void testDictionaryRequiredIndexIsDetectedForRawColumn() {
    String columnName = "rawCol";
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension(columnName, FieldSpec.DataType.STRING)
        .build();
    FieldConfig fieldConfig = new FieldConfig(columnName, FieldConfig.EncodingType.RAW,
        List.of(FieldConfig.IndexType.INVERTED), null, null);
    assertTrue(fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW);

    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.inverted(), IndexConfig.ENABLED)
        .build();

    assertTrue(DictionaryIndexConfig.isDictionaryRequired(schema.getFieldSpecFor(columnName), fieldIndexConfigs));
    assertEquals(DictionaryIndexConfig.getIndexTypesWithDictionaryRequired(schema.getFieldSpecFor(columnName),
        fieldIndexConfigs), List.of(StandardIndexes.inverted()));
  }

  @Test
  public void testRawColumnKeepsRawForwardEncodingWhenDictionaryIsMaterialized() {
    String columnName = "rawCol";
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension(columnName, FieldSpec.DataType.STRING)
        .build();
    FieldConfig fieldConfig = new FieldConfig(columnName, FieldConfig.EncodingType.RAW,
        List.of(FieldConfig.IndexType.INVERTED), null, null);
    TableConfig tableConfig = new org.apache.pinot.spi.utils.builder.TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setFieldConfigList(List.of(fieldConfig))
        .build();

    Map<String, FieldIndexConfigs> configsByCol =
        FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema);
    FieldIndexConfigs fieldIndexConfigs = configsByCol.get(columnName);

    assertTrue(DictionaryIndexConfig.isDictionaryRequired(schema.getFieldSpecFor(columnName), fieldIndexConfigs));
    assertEquals(DictionaryIndexConfig.getIndexTypesWithDictionaryRequired(schema.getFieldSpecFor(columnName),
        fieldIndexConfigs), List.of(StandardIndexes.inverted()));
    assertEquals(fieldIndexConfigs.getConfig(StandardIndexes.forward()).getForwardIndexEncoding(),
        IndexCreationContext.ForwardIndexEncoding.RAW);
  }

  private static class TestIndexPlugin implements IndexPlugin<IndexType<?, ?, ?>> {
    private final IndexType<?, ?, ?> _indexType;

    TestIndexPlugin(IndexType<?, ?, ?> indexType) {
      _indexType = indexType;
    }

    @Override
    public IndexType<?, ?, ?> getIndexType() {
      return _indexType;
    }
  }

  private static class TestDictionaryIndexType
      extends AbstractIndexType<DictionaryIndexConfig, IndexReader, IndexCreator> {
    TestDictionaryIndexType() {
      super(StandardIndexes.DICTIONARY_ID);
    }

    @Override
    public Class<DictionaryIndexConfig> getIndexConfigClass() {
      return DictionaryIndexConfig.class;
    }

    @Override
    public DictionaryIndexConfig getDefaultConfig() {
      return DictionaryIndexConfig.DEFAULT;
    }

    @Override
    protected IndexReaderFactory<IndexReader> createReaderFactory() {
      return new IndexReaderFactory<IndexReader>() {
        @Override
        public IndexReader createIndexReader(org.apache.pinot.segment.spi.store.SegmentDirectory.Reader segmentReader,
            FieldIndexConfigs fieldIndexConfigs, org.apache.pinot.segment.spi.ColumnMetadata metadata) {
          return null;
        }
      };
    }

    @Override
    public IndexHandler createIndexHandler(org.apache.pinot.segment.spi.store.SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      return IndexHandler.NoOp.INSTANCE;
    }

    @Override
    public IndexCreator createIndexCreator(org.apache.pinot.segment.spi.creator.IndexCreationContext context,
        DictionaryIndexConfig indexConfig) {
      throw new UnsupportedOperationException("Not needed for this test");
    }

    @Override
    public List<String> getFileExtensions(org.apache.pinot.segment.spi.ColumnMetadata columnMetadata) {
      return List.of();
    }
  }

  private static class TestInvertedIndexType
      extends AbstractIndexType<IndexConfig, InvertedIndexReader, IndexCreator> {
    TestInvertedIndexType() {
      super(StandardIndexes.INVERTED_ID);
    }

    @Override
    public Class<IndexConfig> getIndexConfigClass() {
      return IndexConfig.class;
    }

    @Override
    public IndexConfig getDefaultConfig() {
      return IndexConfig.DISABLED;
    }

    @Override
    public String getPrettyName() {
      return FieldConfig.IndexType.INVERTED.name().toLowerCase();
    }

    @Override
    protected ColumnConfigDeserializer<IndexConfig> createDeserializer() {
      return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.INVERTED,
          (tableConfig, fieldConfig) -> IndexConfig.ENABLED);
    }

    @Override
    public boolean requiresDictionary(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return true;
    }

    @Override
    protected IndexReaderFactory<InvertedIndexReader> createReaderFactory() {
      return new IndexReaderFactory<InvertedIndexReader>() {
        @Override
        public InvertedIndexReader createIndexReader(
            org.apache.pinot.segment.spi.store.SegmentDirectory.Reader segmentReader,
            FieldIndexConfigs fieldIndexConfigs, org.apache.pinot.segment.spi.ColumnMetadata metadata) {
          return null;
        }
      };
    }

    @Override
    public IndexHandler createIndexHandler(org.apache.pinot.segment.spi.store.SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      return IndexHandler.NoOp.INSTANCE;
    }

    @Override
    public IndexCreator createIndexCreator(org.apache.pinot.segment.spi.creator.IndexCreationContext context,
        IndexConfig indexConfig) {
      throw new UnsupportedOperationException("Not needed for this test");
    }

    @Override
    public List<String> getFileExtensions(org.apache.pinot.segment.spi.ColumnMetadata columnMetadata) {
      return List.of();
    }
  }

  private static class TestForwardIndexType
      extends AbstractIndexType<ForwardIndexConfig, ForwardIndexReader, IndexCreator> {
    TestForwardIndexType() {
      super(StandardIndexes.FORWARD_ID);
    }

    @Override
    public Class<ForwardIndexConfig> getIndexConfigClass() {
      return ForwardIndexConfig.class;
    }

    @Override
    public ForwardIndexConfig getDefaultConfig() {
      return ForwardIndexConfig.getDefault();
    }

    @Override
    protected IndexReaderFactory<ForwardIndexReader> createReaderFactory() {
      return new IndexReaderFactory<ForwardIndexReader>() {
        @Override
        public ForwardIndexReader createIndexReader(
            org.apache.pinot.segment.spi.store.SegmentDirectory.Reader segmentReader,
            FieldIndexConfigs fieldIndexConfigs, org.apache.pinot.segment.spi.ColumnMetadata metadata) {
          return null;
        }
      };
    }

    @Override
    public IndexHandler createIndexHandler(org.apache.pinot.segment.spi.store.SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      return IndexHandler.NoOp.INSTANCE;
    }

    @Override
    public IndexCreator createIndexCreator(org.apache.pinot.segment.spi.creator.IndexCreationContext context,
        ForwardIndexConfig indexConfig) {
      throw new UnsupportedOperationException("Not needed for this test");
    }

    @Override
    public List<String> getFileExtensions(org.apache.pinot.segment.spi.ColumnMetadata columnMetadata) {
      return List.of();
    }
  }
}
