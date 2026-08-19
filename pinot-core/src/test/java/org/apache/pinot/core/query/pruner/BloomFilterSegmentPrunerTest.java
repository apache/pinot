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
package org.apache.pinot.core.query.pruner;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.bloom.OnHeapGuavaBloomFilterReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class BloomFilterSegmentPrunerTest {
  private static final BloomFilterSegmentPruner PRUNER = new BloomFilterSegmentPruner();
  private static final String UUID_COLUMN = "uuidColumn";
  private static final String UUID_0 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String ABSENT_UUID = "550e8400-e29b-41d4-a716-446655440001";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440002";
  private static final String UUID_3 = "550e8400-e29b-41d4-a716-446655440003";

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);
  }

  @Test
  public void testBloomFilterPruning()
      throws IOException {
    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});

    // all out the bloom filter
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 6.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (6.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0 OR column = 6.0"));

    // all in the bloom filter
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 7.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0, 7.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 AND column = 7.0"));

    // some in the bloom filter with IN/OR
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0, 3.0, 4.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 1.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (1.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0, 30.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 OR column = 30.0"));
    // 30 out the bloom filter with AND
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0"));
  }

  @Test(dataProvider = "uuidBloomFilterCreationModes")
  public void testUuidBloomFilterPruningEndToEnd(boolean noDictionary, boolean multiValue, boolean createOnLoad)
      throws Exception {
    File indexDir = Files.createTempDirectory("uuidBloomFilterSegment").toFile();
    ImmutableSegment segment = null;
    try {
      TableConfig tableConfig = createTableConfig(noDictionary, !createOnLoad);
      Schema schema = createSchema(multiValue);

      List<GenericRow> rows = new ArrayList<>();
      rows.add(row(UUID_0, multiValue));
      rows.add(row(UUID_2, multiValue));

      String segmentName = (noDictionary ? "raw" : "dictionary") + (multiValue ? "Mv" : "Sv") + "UuidSegment";
      SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
      generatorConfig.setSegmentName(segmentName);
      generatorConfig.setOutDir(indexDir.getAbsolutePath());
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(generatorConfig, new GenericRowRecordReader(rows));
      driver.build();

      File segmentDir = new File(indexDir, segmentName);
      if (createOnLoad) {
        segment = ImmutableSegmentLoader.load(segmentDir,
            new IndexLoadingConfig(createTableConfig(noDictionary, true), schema));
      } else {
        segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
      }
      assertEquals(segment.getSegmentMetadata().getColumnMetadataFor(UUID_COLUMN).hasDictionary(), !noDictionary);
      assertEquals(segment.getSegmentMetadata().getColumnMetadataFor(UUID_COLUMN).isSingleValue(), !multiValue);
      assertEquals(segment.getDataSource(UUID_COLUMN).getDataSourceMetadata().getDataType(), DataType.UUID);
      assertNotNull(segment.getDataSource(UUID_COLUMN).getBloomFilter());

      assertFalse(runPruner(segment,
          "SELECT COUNT(*) FROM testTable WHERE uuidColumn = '" + UUID_0 + "'"));
      assertFalse(runPruner(segment,
          "SELECT COUNT(*) FROM testTable WHERE uuidColumn = '550E8400-E29B-41D4-A716-446655440000'"));
      assertFalse(runPruner(segment,
          "SELECT COUNT(*) FROM testTable WHERE uuidColumn = '550e8400e29b41d4a716446655440000'"));
      if (multiValue) {
        assertFalse(runPruner(segment,
            "SELECT COUNT(*) FROM testTable WHERE uuidColumn = '" + UUID_3 + "'"));
      }
      assertFalse(runPruner(segment,
          "SELECT COUNT(*) FROM testTable WHERE uuidColumn IN ('" + ABSENT_UUID + "', '" + UUID_2 + "')"));
      assertTrue(runPruner(segment,
          "SELECT COUNT(*) FROM testTable WHERE uuidColumn = '" + ABSENT_UUID + "'"));
    } finally {
      if (segment != null) {
        segment.destroy();
      }
      FileUtils.deleteDirectory(indexDir);
    }
  }

  @DataProvider(name = "uuidBloomFilterCreationModes")
  public Object[][] uuidBloomFilterCreationModes() {
    return new Object[][]{
        {false, false, false},
        {true, false, false},
        {false, true, false},
        {true, true, false},
        {false, false, true},
        {true, false, true},
        {false, true, true},
        {true, true, true}
    };
  }

  private static TableConfig createTableConfig(boolean noDictionary, boolean enableBloomFilter) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable");
    if (noDictionary) {
      builder.setNoDictionaryColumns(List.of(UUID_COLUMN));
    }
    TableConfig tableConfig = builder.build();
    if (enableBloomFilter) {
      tableConfig.getIndexingConfig().setBloomFilterConfigs(
          Map.of(UUID_COLUMN, new BloomFilterConfig(1e-9, 0, false)));
    }
    return tableConfig;
  }

  private static Schema createSchema(boolean multiValue) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    if (multiValue) {
      builder.addMultiValueDimension(UUID_COLUMN, DataType.UUID);
    } else {
      builder.addSingleValueDimension(UUID_COLUMN, DataType.UUID);
    }
    return builder.build();
  }

  private static GenericRow row(String uuid, boolean multiValue) {
    GenericRow row = new GenericRow();
    row.putValue(UUID_COLUMN, multiValue ? new String[]{uuid, UUID_3} : uuid);
    return row;
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testQueryTimeoutOnPruning()
      throws IOException {
    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSourceNullable("column")).thenReturn(dataSource);
    runPruner(List.of(indexSegment),
        "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 0.0", 1);
  }

  @Test
  public void testParallelPrune()
      throws IOException {
    List<IndexSegment> segments = new ArrayList<>();
    for (int i = 0; i < 35; i++) {
      IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});
      segments.add(indexSegment);
    }
    assertTrue(
        runPruner(segments, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0", 5000).isEmpty());

    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0", "30.0"});
    segments.add(indexSegment);
    List<IndexSegment> selected =
        runPruner(segments, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0", 5000);
    assertEquals(selected.size(), 1);
  }

  @Test
  public void testIsApplicableTo() {
    // EQ and IN (with small number of values) are applicable for bloom filter based pruning.
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column = 1");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column IN (1, 2)");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // NOT is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE NOT column = 1");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Too many values for IN clause
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // ... but applicable when the query threshold is negative
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SET inPredicatePruningThreshold=-1; SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    // Other predicate types are not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column LIKE 5");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // AND with one applicable child filter is applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column NOT IN (1, 2) AND column = 3");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // OR with one child filter that's not applicable is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR column NOT IN (1, 2)");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // Nested with AND/OR
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR (column NOT IN (1, 2) AND column = 4)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
  }

  @Test
  public void testInPredicatePruningThresholdOverride()
      throws IOException {
    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});

    // Without the option, a large IN list (more than the default threshold of 10) is not pruned even though all
    // values are absent from the bloom filter
    assertFalse(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0)"));
    // A negative query threshold always prunes, regardless of the IN clause size
    assertTrue(runPruner(indexSegment,
        "SET inPredicatePruningThreshold=-1; SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0)"));
    // A positive query threshold above the clause size also prunes
    assertTrue(runPruner(indexSegment,
        "SET inPredicatePruningThreshold=20; SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0)"));
  }

  @Test
  public void testNegativeServerThresholdAlwaysPrunes()
      throws IOException {
    // The server config follows the same convention: a negative `inpredicate.threshold` always prunes
    BloomFilterSegmentPruner pruner = new BloomFilterSegmentPruner();
    pruner.init(new PinotConfiguration(Map.of(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, -1)));

    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});
    // 11 values, all absent from the bloom filter, pruned without any query option
    assertTrue(runPruner(pruner, indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0)"));
  }

  private IndexSegment mockIndexSegment(String[] values)
      throws IOException {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("column"));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(20);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSourceNullable("column")).thenReturn(dataSource);
    // Add support for bloom filter
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    BloomFilterReaderBuilder builder = new BloomFilterReaderBuilder();
    for (String v : values) {
      builder.put(v);
    }
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.DOUBLE);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getBloomFilter()).thenReturn(builder.build());

    return indexSegment;
  }

  private boolean runPruner(IndexSegment segment, String query) {
    return runPruner(List.of(segment), query, 5000).isEmpty();
  }

  private boolean runPruner(BloomFilterSegmentPruner pruner, IndexSegment segment, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 5000);
    return pruner.prune(List.of(segment), queryContext, Executors.newCachedThreadPool()).isEmpty();
  }

  private List<IndexSegment> runPruner(List<IndexSegment> segments, String query, long queryTimeout) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + queryTimeout);
    return PRUNER.prune(segments, queryContext, Executors.newCachedThreadPool());
  }

  private static class BloomFilterReaderBuilder {
    private BloomFilter<String> _bloomfilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100, 0.01);
    public BloomFilterReaderBuilder put(String value) {
      _bloomfilter.put(value);
      return this;
    }

    public BloomFilterReader build() throws IOException {
      File file = Files.createTempFile("test", ".bloom").toFile();
      try (FileOutputStream fos = new FileOutputStream(file)) {
        _bloomfilter.writeTo(fos);
        try (PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(file)) {
          // on heap filter should never use the buffer, so we can close it and delete the file
          return new OnHeapGuavaBloomFilterReader(pinotDataBuffer);
        }
      } finally {
        file.delete();
      }
    }
  }
}
