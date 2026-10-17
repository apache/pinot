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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests filtering of records during segment generation
public class SegmentGenerationWithMinMaxTest implements PinotBuffersAfterMethodCheckRule {
  private static final String STRING_COLUMN = "col1";

  private static final String[] STRING_VALUES_WITH_COMMA_CHARACTER = {"A,,", ",B,", "C,Z,", "D,", "E,"};
  private static final String[] STRING_VALUES_WITH_WHITESPACE_CHARACTERS = {"A ", " B ", "  Z ", "  \r D", "E"};
  private static final String[] STRING_VALUES_VALID = {"A", "B", "C", "D", "E"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES =
      {1588316400000L, 1588489200000L, 1588662000000L, 1588834800000L, 1589007600000L};

  private static final String SEGMENT_DIR_NAME =
      FileUtils.getTempDirectoryPath() + File.separator + "segmentMinMaxTest";
  private static final String SEGMENT_NAME = "testSegmentMinMax";

  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).build();
  }

  @Test
  public void testMinMaxlength() throws Exception {
    //Test1: Min String length is greater than default 512
    DimensionFieldSpec d = new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true, 15000, "null");
    _schema = new Schema.SchemaBuilder().addField(d)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).build();

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    //minLong stRING
    String longString = generateLongString(15000, true);
    String[] longValues = {"{}", "dd", "ff", "ee", longString};
    File segmentDir = buildSegment(_tableConfig, _schema, longValues);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    assertEquals(metadata.getTotalDocs(), 5);
    assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    assertNull(metadata.getColumnMetadataFor("col1").getMinValue());
    assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "{}");

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));

    //maxLong String
    longString = generateLongString(15000, false);
    longValues = new String[]{"aa", "dd", "ff", "ee", longString};
    segmentDir = buildSegment(_tableConfig, _schema, longValues);
    metadata = new SegmentMetadataImpl(segmentDir);
    assertEquals(metadata.getTotalDocs(), 5);
    assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), "aa");
    assertNull(metadata.getColumnMetadataFor("col1").getMaxValue());

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void testMinMaxInMetadata()
      throws Exception {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    File segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_VALID);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    assertEquals(metadata.getTotalDocs(), 5);
    assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), "A");
    assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E");

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_WITH_COMMA_CHARACTER);
    metadata = new SegmentMetadataImpl(segmentDir);
    assertEquals(metadata.getTotalDocs(), 5);
    assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), ",B,");
    assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E,");

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_WITH_WHITESPACE_CHARACTERS);
    metadata = new SegmentMetadataImpl(segmentDir);
    assertEquals(metadata.getTotalDocs(), 5);
    assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), "  \r D");
    assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E");
  }

  @DataProvider
  public Object[][] deferredRawWriterVersions() {
    return new Object[][]{{4}, {6}};
  }

  @Test(dataProvider = "deferredRawWriterVersions")
  public void testDeferredRawStatsPreserveValuesMetadataAndText(int writerVersion)
      throws Exception {
    File directory = Files.createTempDirectory("deferred-raw-stats").toFile();
    try {
      Schema schema = deferredRawSchema();
      schema.addField(new DimensionFieldSpec("time", DataType.LONG, true));
      TableConfig table = deferredRawTable(writerVersion);
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setTransformConfigs(List.of(new TransformConfig("time", "sourceTime")));
      table.setIngestionConfig(ingestion);
      AtomicInteger legacySerializations = new AtomicInteger();
      AtomicInteger deferredSerializations = new AtomicInteger();
      String hugeValue = "é".repeat(1_100_000) + "complete";
      File legacy = buildRawStatsSegment(directory, "legacy", table, schema,
          new GenericRowRecordReader(deferredRawRows(2500, hugeValue, legacySerializations)), false)
          .getOutputDirectory();
      ProjectingRecordReader projectingReader =
          new ProjectingRecordReader(deferredRawRows(2500, hugeValue, deferredSerializations));
      File deferred = buildRawStatsSegment(directory, "deferred", table, schema, projectingReader, true)
          .getOutputDirectory();
      assertEquals(projectingReader._requestedFields, Set.of("service", "time", "sourceTime"));
      assertTrue(projectingReader._restoredAfterProjection);

      // Maps are serialized only in the index pass; the old path serializes the same maps in both passes.
      assertTrue(deferredSerializations.get() > 0);
      assertEquals(legacySerializations.get(), deferredSerializations.get() * 2);
      ImmutableSegment first = ImmutableSegmentLoader.load(legacy, ReadMode.mmap);
      ImmutableSegment second = ImmutableSegmentLoader.load(deferred, ReadMode.mmap);
      try {
        assertEquals(second.getSegmentMetadata().getTotalDocs(), 2500);
        for (String column : schema.getPhysicalColumnNames()) {
          assertColumnShapeEquals(first.getSegmentMetadata().getColumnMetadataFor(column),
              second.getSegmentMetadata().getColumnMetadataFor(column));
          try (PinotSegmentColumnReader oldReader = new PinotSegmentColumnReader(first, column);
              PinotSegmentColumnReader newReader = new PinotSegmentColumnReader(second, column)) {
            for (int docId = 0; docId < 2500; docId++) {
              assertEquals(newReader.getValue(docId), oldReader.getValue(docId), column + " doc " + docId);
              assertEquals(newReader.isNull(docId), oldReader.isNull(docId), column + " null " + docId);
            }
          }
        }
        try (PinotSegmentColumnReader reader = new PinotSegmentColumnReader(second, "huge")) {
          assertEquals(reader.getValue(0), hugeValue, "A value larger than the V4/V6 chunk must remain complete");
        }
        assertEquals(second.getSegmentMetadata().getColumnMetadataFor("body").getLengthOfLongestElement(), 512);
        assertTrue(second.getSegmentMetadata().getColumnMetadataFor("sorted").isSorted());
        assertFalse(second.getSegmentMetadata().getColumnMetadataFor("attributes").hasDictionary());
        assertTrue(second.getSegmentMetadata().getColumnMetadataFor("attributes").getCardinality() > 2048);
        for (String query : List.of("needle", "tailword", "null")) {
          assertEquals(second.getDataSource("body").getTextIndex().getDocIds(query, null).toArray(),
              first.getDataSource("body").getTextIndex().getDocIds(query, null).toArray());
        }
        assertEquals(second.getDataSource("body").getTextIndex().getDocIds("tailword", null).getCardinality(), 0);
      } finally {
        first.destroy();
        second.destroy();
      }
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @DataProvider
  public Object[][] deferredRawUnsupportedPipelines() {
    return new Object[][]{{"filter"}, {"continueOnError"}, {"rawDependency"}, {"rawTransform"}, {"legacyWriter"},
        {"dynamicChunks"}, {"customPipeline"}};
  }

  @Test
  public void testDeferredRawStatsRestoreProjectionAfterReadFailure()
      throws Exception {
    File directory = Files.createTempDirectory("deferred-projection-failure").toFile();
    try {
      ProjectingRecordReader reader = new ProjectingRecordReader(deferredRawRows(4, "small", new AtomicInteger()));
      reader._failProjection = true;
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(deferredRawTable(4), deferredRawSchema());
      config.setOutDir(directory.getAbsolutePath());
      config.setSegmentName("restored");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, reader);
      expectThrows(RuntimeException.class, driver::build);
      assertTrue(reader._restoredAfterProjection);
      assertEquals(reader.next().getValue("huge"), "small", "Failure must restore full fields and the first row");
      reader.rewind();
      driver.build();
      assertEquals(new SegmentMetadataImpl(driver.getOutputDirectory()).getTotalDocs(), 4);
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test(dataProvider = "deferredRawUnsupportedPipelines")
  public void testDeferredRawStatsFallBackForUnsupportedPipelines(String unsupported)
      throws Exception {
    File directory = Files.createTempDirectory("deferred-raw-fallback").toFile();
    try {
      TableConfig table = deferredRawTable(unsupported.equals("legacyWriter") ? 2 : 4,
          !unsupported.equals("dynamicChunks"));
      IngestionConfig ingestion = new IngestionConfig();
      table.setIngestionConfig(ingestion);
      switch (unsupported) {
        case "filter":
          ingestion.setFilterConfig(new FilterConfig("strcmp(service, 'never') = 0"));
          break;
        case "continueOnError":
          ingestion.setContinueOnError(true);
          break;
        case "rawDependency":
          ingestion.setTransformConfigs(List.of(new TransformConfig("service", "body")));
          break;
        case "rawTransform":
          ingestion.setTransformConfigs(List.of(new TransformConfig("body", "sourceBody")));
          break;
        default:
          break;
      }
      AtomicInteger serializations = new AtomicInteger();
      List<GenericRow> rows = deferredRawRows(4, "small", serializations);
      buildRawStatsSegment(directory, "fallback", table, deferredRawSchema(), new GenericRowRecordReader(rows),
          !unsupported.equals("customPipeline"));
      assertEquals(serializations.get(), rows.size() * 2, "JSON must retain the legacy two-pass path: " + unsupported);
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void testDeferredRawStatsReuseCapturedNullTimeDefault()
      throws Exception {
    File directory = Files.createTempDirectory("deferred-null-time").toFile();
    try {
      Schema schema = deferredRawSchema();
      schema.addField(new DateTimeFieldSpec("time", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS"));
      TableConfig table = deferredRawTable(4);
      table.getValidationConfig().setTimeColumnName("time");
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setTransformConfigs(List.of(new TransformConfig("time", "sourceTime")));
      table.setIngestionConfig(ingestion);
      List<GenericRow> rows = deferredRawRows(1, "small", new AtomicInteger());
      rows.getFirst().removeValue("time");
      rows.getFirst().removeValue("sourceTime");
      RecordReader reader = new GenericRowRecordReader(rows) {
        private boolean _firstCheck = true;

        @Override
        public boolean hasNext() {
          if (_firstCheck) {
            _firstCheck = false;
            try {
              // init's empty-input check separates the original null transformer from stats-pipeline construction.
              Thread.sleep(30);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }
          return super.hasNext();
        }
      };
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(table, schema);
      config.setOutDir(directory.getAbsolutePath());
      config.setSegmentName("nullTime");
      config.setFailOnEmptySegment(true);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, reader);
      driver.build();
      ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
      try (PinotSegmentColumnReader timeReader = new PinotSegmentColumnReader(segment, "time")) {
        Object time = timeReader.getValue(0);
        assertEquals(driver.getSegmentStats().getColumnProfileFor("time").getMinValue(), time);
        assertEquals(driver.getSegmentStats().getColumnProfileFor("time").getMaxValue(), time);
      } finally {
        segment.destroy();
      }
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  private static Schema deferredRawSchema() {
    DimensionFieldSpec huge = new DimensionFieldSpec("huge", DataType.STRING, true);
    huge.setMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.NO_ACTION);
    return new Schema.SchemaBuilder().addSingleValueDimension("service", DataType.STRING)
        .addSingleValueDimension("body", DataType.STRING).addSingleValueDimension("attributes", DataType.JSON)
        .addSingleValueDimension("sorted", DataType.STRING).addField(huge).build();
  }

  private static TableConfig deferredRawTable(int writerVersion)
      throws Exception {
    return deferredRawTable(writerVersion, true);
  }

  private static TableConfig deferredRawTable(int writerVersion, boolean fixedChunks)
      throws Exception {
    List<FieldConfig> fields = new ArrayList<>();
    for (String column : List.of("body", "attributes", "sorted", "huge")) {
      Map<String, Object> forwardConfig = new LinkedHashMap<>();
      forwardConfig.put("rawIndexWriterVersion", writerVersion);
      if (fixedChunks && writerVersion >= 4) {
        forwardConfig.put("targetDocsPerChunk", -1);
      }
      fields.add(new FieldConfig(column, FieldConfig.EncodingType.RAW, null,
          column.equals("body") ? List.of(FieldConfig.IndexType.TEXT) : List.of(), FieldConfig.CompressionCodec.LZ4,
          null, JsonUtils.objectToJsonNode(Map.of("forward", forwardConfig)),
          Map.of(FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB, "16"), null));
    }
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("rawStats")
        .setNoDictionaryColumns(List.of("body", "attributes", "sorted", "huge"))
        .setInvertedIndexColumns(List.of("service")).setFieldConfigList(fields).setNullHandlingEnabled(true)
        .setOptimizeNoDictStatsCollection(true).build();
  }

  private static List<GenericRow> deferredRawRows(int count, String hugeValue, AtomicInteger serializations) {
    List<GenericRow> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      CountingJsonMap attributes = new CountingJsonMap(serializations);
      attributes.put("number", i);
      attributes.put("value", "é\\\"" + i);
      GenericRow row = new GenericRow();
      row.putValue("service", "service" + i % 3);
      row.putValue("sourceTime", 2000L + i);
      if (i % 2 == 0) {
        row.putValue("time", 1000L + i);
      }
      row.putValue("body", i % 13 == 0 ? null : "needle " + "x".repeat(600) + " tailword");
      row.putValue("attributes", attributes);
      row.putValue("huge", i == 0 ? hugeValue : "small" + i);
      row.putValue("sorted", String.format("%08d", i));
      rows.add(row);
    }
    return rows;
  }

  private static SegmentIndexCreationDriverImpl buildRawStatsSegment(File directory, String name, TableConfig table,
      Schema schema, RecordReader reader, boolean deferred)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(table, schema);
    config.setOutDir(directory.getAbsolutePath());
    config.setSegmentName(name);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    if (deferred) {
      driver.init(config, reader);
    } else {
      // An explicitly supplied pipeline keeps the old path, with the same optimized no-dictionary collector.
      driver.init(config, new RecordReaderSegmentCreationDataSource(reader), new TransformPipeline(table, schema));
    }
    driver.build();
    return driver;
  }

  private static void assertColumnShapeEquals(ColumnMetadata oldMetadata, ColumnMetadata newMetadata) {
    assertEquals(newMetadata.getFieldSpec(), oldMetadata.getFieldSpec());
    assertEquals(newMetadata.getTotalDocs(), oldMetadata.getTotalDocs());
    assertEquals(newMetadata.getTotalNumberOfEntries(), oldMetadata.getTotalNumberOfEntries());
    assertEquals(newMetadata.getCardinality(), oldMetadata.getCardinality());
    assertEquals(newMetadata.getLengthOfShortestElement(), oldMetadata.getLengthOfShortestElement());
    assertEquals(newMetadata.getLengthOfLongestElement(), oldMetadata.getLengthOfLongestElement());
    assertEquals(newMetadata.getMaxRowLengthInBytes(), oldMetadata.getMaxRowLengthInBytes());
    assertEquals(newMetadata.getMaxNumberOfMultiValues(), oldMetadata.getMaxNumberOfMultiValues());
    assertEquals(newMetadata.isSorted(), oldMetadata.isSorted());
    assertEquals(newMetadata.isAscii(), oldMetadata.isAscii());
    assertEquals(newMetadata.hasDictionary(), oldMetadata.hasDictionary());
    assertEquals(newMetadata.getForwardIndexEncoding(), oldMetadata.getForwardIndexEncoding());
    assertEquals(newMetadata.getBitsPerElement(), oldMetadata.getBitsPerElement());
    assertEquals(newMetadata.getMinValue(), oldMetadata.getMinValue());
    assertEquals(newMetadata.getMaxValue(), oldMetadata.getMaxValue());
    assertEquals(newMetadata.isMinMaxValueInvalid(), oldMetadata.isMinMaxValueInvalid());
    assertEquals(newMetadata.getPartitions(), oldMetadata.getPartitions());
    assertEquals(newMetadata.isNonNull(), oldMetadata.isNonNull());
  }

  /// Counts map serialization without inspecting the private driver implementation.
  private static final class CountingJsonMap extends LinkedHashMap<String, Object> {
    private final AtomicInteger _serializations;

    private CountingJsonMap(AtomicInteger serializations) {
      _serializations = serializations;
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
      _serializations.incrementAndGet();
      return super.entrySet();
    }
  }

  /// Exercises the optional projection contract while retaining the normal in-memory reader otherwise.
  private static final class ProjectingRecordReader extends GenericRowRecordReader {
    private Set<String> _projection;
    private Set<String> _requestedFields;
    private boolean _restoredAfterProjection;
    private boolean _failProjection;

    private ProjectingRecordReader(List<GenericRow> rows) {
      super(rows);
    }

    @Override
    public boolean rewind(Set<String> fieldsToRead) {
      super.rewind();
      _projection = Set.copyOf(fieldsToRead);
      _requestedFields = _projection;
      return true;
    }

    @Override
    public void rewind() {
      _restoredAfterProjection |= _projection != null;
      _projection = null;
      super.rewind();
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      super.next(reuse);
      if (_projection != null) {
        if (_failProjection) {
          _failProjection = false;
          throw new IllegalStateException("Injected projected read failure");
        }
        reuse.getFieldToValueMap().keySet().retainAll(_projection);
      }
      return reuse;
    }
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema, String[] stringValues)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < 5; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, stringValues[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }

  private String generateLongString(int length, boolean isMin) {
    char repeatingChar = isMin ? 'a' : 'z';
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(repeatingChar);
    }
    return sb.toString();
  }
}
