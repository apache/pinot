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
package org.apache.pinot.segment.local.segment.index.column;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexType;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexPlugin;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PhysicalColumnIndexContainerTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), PhysicalColumnIndexContainerTest.class.getSimpleName());

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COL = "intColumn";
  private static final String LONG_COL = "longColumn";
  private static final String FLOAT_COL = "floatColumn";
  private static final String DOUBLE_COL = "doubleColumn";
  private static final String STR_COL = "stringColumn";

  private static final Schema
      SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
      .addSingleValueDimension(FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING)
      .build();

  private static final TableConfig TABLE_CONFIG;

  static {
    ObjectNode indexes = JsonUtils.newObjectNode();
    JsonIndexConfig config = new JsonIndexConfig();
    indexes.set(JsonIndexType.INDEX_DISPLAY_NAME, config.toJsonNode());

    TABLE_CONFIG =
        new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .addFieldConfig(new FieldConfig.Builder(STR_COL)
                .withIndexes(indexes)
                .build())
            .addFieldConfig(new FieldConfig.Builder(INT_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.RANGE))
                .build())
            .addFieldConfig(new FieldConfig.Builder(LONG_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.RANGE))
                .build())
            .addFieldConfig(new FieldConfig.Builder(FLOAT_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.RANGE, FieldConfig.IndexType.SORTED))
                .build())
            .setRangeIndexColumns(List.of(LONG_COL, FLOAT_COL))
            .build();
  }

  private static final String COLUMN = "column";

  // Every standard index id. IndexService assigns numeric ids by sorted id, so the order here is the numeric-id order:
  // bloom_filter = 0, dictionary = 1, forward_index = 2, ..., nullvalue_vector = 8, ..., vector_index = 12.
  private static final List<String> STANDARD_IDS =
      List.of(StandardIndexes.BLOOM_FILTER_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID,
          StandardIndexes.FST_ID, StandardIndexes.H3_ID, StandardIndexes.IFST_ID, StandardIndexes.INVERTED_ID,
          StandardIndexes.JSON_ID, StandardIndexes.NULL_VALUE_VECTOR_ID, StandardIndexes.OPEN_STRUCT_ID,
          StandardIndexes.RANGE_ID, StandardIndexes.TEXT_ID, StandardIndexes.VECTOR_ID);

  private IndexService _originalIndexService;
  private final Map<String, StubIndexType> _stubTypes = new HashMap<>();
  private final Map<String, IndexReader> _stubReaders = new HashMap<>();

  @BeforeClass
  public void saveIndexService() {
    _originalIndexService = IndexService.getInstance();
  }

  @AfterMethod
  public void restoreIndexService() {
    IndexService.setInstance(_originalIndexService);
    _stubTypes.clear();
    _stubReaders.clear();
  }

  @Test
  public void testCreateSegmentAndCheckColumnIndexes()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    StringBuilder sb = new StringBuilder();
    List<GenericRow> records = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      sb.append("{ \"value\": ").append(i).append(" }");

      GenericRow record = new GenericRow();
      record.putValue(INT_COL, i);
      record.putValue(LONG_COL, (long) i);
      record.putValue(FLOAT_COL, (float) i);
      record.putValue(DOUBLE_COL, (double) i);
      record.putValue(STR_COL, sb.toString());
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = null;
    try {
      segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.json()));
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.forward()));
      assertNull(segment.getIndex(STR_COL, StandardIndexes.range()));

      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.forward()));
      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.range()));
      assertNull(segment.getIndex(FLOAT_COL, StandardIndexes.json()));

      assertNotNull(segment.getIndex(DOUBLE_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(DOUBLE_COL, StandardIndexes.forward()));
      assertNull(segment.getIndex(DOUBLE_COL, StandardIndexes.range()));

      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.forward()));
      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.range()));

      // Index types that no column of this segment has resolve to null for every column. Every column of this
      // fixture is sorted, so the inverted index resolves to the sorted reader rather than null.
      for (String column : SCHEMA.getColumnNames()) {
        assertNotNull(segment.getIndex(column, StandardIndexes.inverted()));
        assertNull(segment.getIndex(column, StandardIndexes.bloomFilter()));
        assertNull(segment.getIndex(column, StandardIndexes.nullValueVector()));
        assertNull(segment.getIndex(column, StandardIndexes.text()));
        assertNull(segment.getIndex(column, StandardIndexes.vector()));
      }
    } finally {
      if (segment != null) {
        segment.destroy();
      }
    }
  }

  @Test
  public void testForwardOnlyColumn()
      throws IOException {
    installStandardStubs();
    PhysicalColumnIndexContainer container = newContainer(Set.of(StandardIndexes.FORWARD_ID), false);
    assertPresentExactly(container, Set.of(StandardIndexes.FORWARD_ID));
  }

  @Test
  public void testForwardAndNullValueVector()
      throws IOException {
    // Numeric ids 2 and 8: the span array of the old layout would have held 7 slots for these 2 readers.
    installStandardStubs();
    Set<String> present = Set.of(StandardIndexes.FORWARD_ID, StandardIndexes.NULL_VALUE_VECTOR_ID);
    assertPresentExactly(newContainer(present, false), present);
  }

  @Test
  public void testLowestAndHighIds()
      throws IOException {
    // bloom_filter is numeric id 0 and vector_index is the highest standard id.
    installStandardStubs();
    Set<String> present =
        Set.of(StandardIndexes.BLOOM_FILTER_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID,
            StandardIndexes.JSON_ID, StandardIndexes.RANGE_ID, StandardIndexes.VECTOR_ID);
    assertPresentExactly(newContainer(present, false), present);
  }

  @Test
  public void testSparseNonAdjacentIds()
      throws IOException {
    installStandardStubs();
    Set<String> present = Set.of(StandardIndexes.BLOOM_FILTER_ID, StandardIndexes.JSON_ID, StandardIndexes.VECTOR_ID);
    assertPresentExactly(newContainer(present, false), present);
  }

  @Test
  public void testAllIndexesPresent()
      throws IOException {
    installStandardStubs();
    assertPresentExactly(newContainer(new HashSet<>(STANDARD_IDS), false), new HashSet<>(STANDARD_IDS));
  }

  @Test
  public void testNoIndexes()
      throws IOException {
    installStandardStubs();
    PhysicalColumnIndexContainer container = newContainer(Set.of(), false);
    assertPresentExactly(container, Set.of());
    container.close();
    for (IndexReader reader : _stubReaders.values()) {
      verify(reader, never()).close();
    }
  }

  @Test
  public void testForwardIndexOnlyFiltering()
      throws IOException {
    installStandardStubs();
    SegmentDirectory.Reader segmentReader = mockSegmentReader(new HashSet<>(STANDARD_IDS));
    PhysicalColumnIndexContainer container = newContainer(segmentReader, true);
    Set<String> expected =
        Set.of(StandardIndexes.FORWARD_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.NULL_VALUE_VECTOR_ID);
    assertPresentExactly(container, expected);
    // Filtered types are skipped before the segment reader is consulted.
    for (String id : STANDARD_IDS) {
      if (!expected.contains(id)) {
        verify(segmentReader, never()).hasIndexFor(COLUMN, _stubTypes.get(id));
      }
    }
  }

  @Test
  public void testFactoryReturningNullIsAbsent()
      throws IOException {
    installStandardStubs();
    _stubTypes.get(StandardIndexes.DICTIONARY_ID).setReaderFactory((reader, configs, metadata) -> null);
    Set<String> present =
        Set.of(StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID, StandardIndexes.RANGE_ID);
    assertPresentExactly(newContainer(present, false),
        Set.of(StandardIndexes.FORWARD_ID, StandardIndexes.RANGE_ID));
  }

  @Test
  public void testConstraintViolationSkipsIndex()
      throws IOException {
    installStandardStubs();
    StubIndexType rangeType = _stubTypes.get(StandardIndexes.RANGE_ID);
    rangeType.setReaderFactory((reader, configs, metadata) -> {
      throw new IndexReaderConstraintException(COLUMN, rangeType, "no dictionary");
    });
    Set<String> present =
        Set.of(StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID, StandardIndexes.RANGE_ID,
            StandardIndexes.TEXT_ID);
    assertPresentExactly(newContainer(present, false),
        Set.of(StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID, StandardIndexes.TEXT_ID));
  }

  @Test
  public void testCloseClosesEveryReaderOnce()
      throws IOException {
    installStandardStubs();
    Set<String> present =
        Set.of(StandardIndexes.BLOOM_FILTER_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID,
            StandardIndexes.NULL_VALUE_VECTOR_ID, StandardIndexes.VECTOR_ID);
    PhysicalColumnIndexContainer container = newContainer(present, false);
    MultiColumnLuceneTextIndexReader multiColTextReader = mock(MultiColumnLuceneTextIndexReader.class);
    container.setMultiColumnTextIndex(multiColTextReader);
    assertSame(container.getMultiColumnTextIndex(), multiColTextReader);

    container.close();

    for (String id : STANDARD_IDS) {
      if (present.contains(id)) {
        verify(_stubReaders.get(id)).close();
      } else {
        verify(_stubReaders.get(id), never()).close();
      }
    }
    // The multi-column text reader is shared across columns and closed by the segment, not the container.
    verify(multiColTextReader, never()).close();
    assertNull(container.getMultiColumnTextIndex());
  }

  @Test
  public void testInitFailureClosesCreatedReaders()
      throws IOException {
    installStandardStubs();
    IOException failure = new IOException("cannot open json index");
    _stubTypes.get(StandardIndexes.JSON_ID).setReaderFactory((reader, configs, metadata) -> {
      throw failure;
    });
    Set<String> present =
        Set.of(StandardIndexes.BLOOM_FILTER_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.FORWARD_ID,
            StandardIndexes.JSON_ID, StandardIndexes.RANGE_ID);
    SegmentDirectory.Reader segmentReader = mockSegmentReader(present);

    IOException thrown = expectThrows(IOException.class, () -> newContainer(segmentReader, false));
    assertSame(thrown, failure);

    // Readers created before the failure (lower numeric ids) are closed; the rest were never created.
    verify(_stubReaders.get(StandardIndexes.BLOOM_FILTER_ID)).close();
    verify(_stubReaders.get(StandardIndexes.DICTIONARY_ID)).close();
    verify(_stubReaders.get(StandardIndexes.FORWARD_ID)).close();
    verify(segmentReader, never()).hasIndexFor(COLUMN, _stubTypes.get(StandardIndexes.RANGE_ID));
    verify(_stubReaders.get(StandardIndexes.RANGE_ID), never()).close();
  }

  @Test
  public void testTooManyIndexTypesFailsConstruction() {
    installIndexService(standardAndSyntheticTypes(Long.SIZE + 1));
    assertEquals(IndexService.getInstance().getAllIndexes().size(), Long.SIZE + 1);

    SegmentDirectory.Reader segmentReader = mock(SegmentDirectory.Reader.class);
    IllegalStateException thrown =
        expectThrows(IllegalStateException.class, () -> newContainer(segmentReader, false));
    assertTrue(thrown.getMessage().contains("65 index types"), thrown.getMessage());
    verify(segmentReader, never()).hasIndexFor(any(), any());
  }

  @Test
  public void testMaximumIndexTypesFitInMask()
      throws IOException {
    List<StubIndexType> types = standardAndSyntheticTypes(Long.SIZE);
    installIndexService(types);
    IndexService indexService = IndexService.getInstance();
    assertEquals(indexService.getAllIndexes().size(), Long.SIZE);

    // The two extreme numeric ids (0 and 63) plus one in the middle.
    Set<String> present = new HashSet<>();
    for (int numericId : new int[]{0, 31, Long.SIZE - 1}) {
      present.add(indexService.get(numericId).getId());
    }
    PhysicalColumnIndexContainer container = newContainer(mockSegmentReader(present), false);
    for (StubIndexType type : types) {
      if (present.contains(type.getId())) {
        assertSame(container.getIndex(type), _stubReaders.get(type.getId()), type.getId());
      } else {
        assertNull(container.getIndex(type), type.getId());
      }
    }
  }

  /// Installs an IndexService whose types carry every standard index id, so numeric ids match production, backed by
  /// one mock reader per type.
  private void installStandardStubs() {
    installIndexService(standardAndSyntheticTypes(STANDARD_IDS.size()));
  }

  /// The standard ids (so [StandardIndexes] accessors resolve) padded with synthetic ids up to the given count.
  private static List<StubIndexType> standardAndSyntheticTypes(int numTypes) {
    List<StubIndexType> types = new ArrayList<>();
    for (String id : STANDARD_IDS) {
      types.add(new StubIndexType(id));
    }
    for (int i = STANDARD_IDS.size(); i < numTypes; i++) {
      types.add(new StubIndexType(String.format("synthetic_index_%02d", i)));
    }
    return types;
  }

  private void installIndexService(List<StubIndexType> types) {
    Set<IndexPlugin<?>> plugins = new HashSet<>();
    for (StubIndexType type : types) {
      IndexReader reader = mock(IndexReader.class, type.getId());
      _stubReaders.put(type.getId(), reader);
      _stubTypes.put(type.getId(), type);
      type.setReaderFactory((segmentReader, configs, metadata) -> reader);
      plugins.add((IndexPlugin<StubIndexType>) () -> type);
    }
    IndexService.setInstance(new IndexService(plugins));
  }

  private SegmentDirectory.Reader mockSegmentReader(Set<String> presentIds) {
    SegmentDirectory.Reader segmentReader = mock(SegmentDirectory.Reader.class);
    when(segmentReader.hasIndexFor(eq(COLUMN), any())).thenAnswer(
        invocation -> presentIds.contains(((IndexType<?, ?, ?>) invocation.getArgument(1)).getId()));
    return segmentReader;
  }

  private PhysicalColumnIndexContainer newContainer(Set<String> presentIds, boolean forwardIndexOnly)
      throws IOException {
    return newContainer(mockSegmentReader(presentIds), forwardIndexOnly);
  }

  private static PhysicalColumnIndexContainer newContainer(SegmentDirectory.Reader segmentReader,
      boolean forwardIndexOnly)
      throws IOException {
    ColumnMetadata metadata = mock(ColumnMetadata.class);
    when(metadata.getColumnName()).thenReturn(COLUMN);
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.isForwardIndexOnly()).thenReturn(forwardIndexOnly);
    return new PhysicalColumnIndexContainer(segmentReader, metadata, indexLoadingConfig);
  }

  /// Asserts that getIndex returns the created reader for every present standard id and null for the others.
  private void assertPresentExactly(PhysicalColumnIndexContainer container, Set<String> presentIds) {
    for (String id : STANDARD_IDS) {
      StubIndexType type = _stubTypes.get(id);
      if (presentIds.contains(id)) {
        assertSame(container.getIndex(type), _stubReaders.get(id), id);
      } else {
        assertNull(container.getIndex(type), id);
      }
    }
  }

  private static final class StubIndexType implements IndexType<IndexConfig, IndexReader, IndexCreator> {
    private final String _id;
    private IndexReaderFactory<IndexReader> _factory;

    StubIndexType(String id) {
      _id = id;
    }

    void setReaderFactory(IndexReaderFactory<IndexReader> factory) {
      _factory = factory;
    }

    @Override
    public String getId() {
      return _id;
    }

    @Override
    public Class<IndexConfig> getIndexConfigClass() {
      return IndexConfig.class;
    }

    @Override
    public IndexConfig getDefaultConfig() {
      // The container reads the vector config through StandardIndexes.vector(), typed as VectorIndexConfig.
      return _id.equals(StandardIndexes.VECTOR_ID) ? VectorIndexConfig.DISABLED : IndexConfig.DISABLED;
    }

    @Override
    public Map<String, IndexConfig> getConfig(TableConfig tableConfig, Schema schema) {
      return Map.of();
    }

    @Override
    public IndexCreator createIndexCreator(IndexCreationContext context, IndexConfig indexConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexReaderFactory<IndexReader> getReaderFactory() {
      return _factory;
    }

    @Override
    public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
      return List.of();
    }

    @Override
    public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean requiresDictionary(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return false;
    }

    @Override
    public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return false;
    }

    @Override
    public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
    }

    @Override
    public String toString() {
      return _id;
    }
  }
}
