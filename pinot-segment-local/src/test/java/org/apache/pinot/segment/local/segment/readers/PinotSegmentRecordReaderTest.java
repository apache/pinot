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
package org.apache.pinot.segment.local.segment.readers;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Segment.COMPARISON_COLUMN;


/// Tests the PinotSegmentRecordReader to check that the records being generated
/// are the same as the records used to create the segment
public class PinotSegmentRecordReaderTest {
  private static final int NUM_ROWS = 10000;
  private static final String D_SV_1 = "d_sv_1";
  private static final String D_MV_1 = "d_mv_1";
  private static final String M1 = "m1";
  private static final String M2 = "m2";
  private static final String TIME = "t";

  private String _segmentOutputDir;
  private File _segmentIndexDir;
  private File _rawNoDictSegmentIndexDir;
  private List<GenericRow> _rows;
  private RecordReader _recordReader;

  @BeforeClass
  public void setup()
      throws Exception {
    Schema schema = createPinotSchema();
    TableConfig tableConfig = createTableConfig();
    String segmentName = "pinotSegmentRecordReaderTest";
    _segmentOutputDir = Files.createTempDirectory("pinot-test-").toFile().toString();
    _rows = PinotSegmentUtil.createTestData(schema, NUM_ROWS);
    _recordReader = new GenericRowRecordReader(_rows);
    _segmentIndexDir =
        PinotSegmentUtil.createSegment(tableConfig, schema, segmentName, _segmentOutputDir, _recordReader);

    TableConfig rawNoDictTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME)
            .setNoDictionaryColumns(List.of(D_SV_1)).build();
    _rawNoDictSegmentIndexDir = PinotSegmentUtil.createSegment(rawNoDictTableConfig, schema,
        segmentName + "_raw_no_dict", _segmentOutputDir, new GenericRowRecordReader(_rows));
  }

  private Schema createPinotSchema() {
    return new Schema.SchemaBuilder().setSchemaName("schema").addSingleValueDimension(D_SV_1, DataType.STRING)
        .addMultiValueDimension(D_MV_1, DataType.STRING).addMetric(M1, DataType.INT).addMetric(M2, DataType.FLOAT)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, TIME), null).build();
  }

  private TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME).build();
  }

  @Test
  public void testPinotSegmentRecordReader()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_segmentIndexDir)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of _rows returned by PinotSegmentRecordReader is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = _rows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
      Assert.assertTrue(PinotSegmentUtil.compareMultiValueColumn(outputRow.getValue(D_MV_1), row.getValue(D_MV_1)));
      Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
      Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
      Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
    }
  }

  @Test
  public void testPinotSegmentRecordReaderSortedColumn()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();
    List<String> sortOrder = new ArrayList<>();
    sortOrder.add(D_SV_1);

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_segmentIndexDir, null,
        sortOrder)) {
      while (pinotSegmentRecordReader.hasNext()) {
        GenericRow row = pinotSegmentRecordReader.next();
        outputRows.add(row);
      }
    }
    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of _rows returned by PinotSegmentRecordReader is incorrect");

    // Check that the _rows are sorted based on sorted column
    GenericRow prev = outputRows.get(0);
    for (int i = 1; i < outputRows.size(); i++) {
      GenericRow current = outputRows.get(i);
      Assert.assertTrue(((String) prev.getValue(D_SV_1)).compareTo((String) current.getValue(D_SV_1)) <= 0);
      prev = current;
    }
  }

  @Test
  public void testPinotSegmentRecordReaderSortedRawStringColumn()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();
    List<String> sortOrder = new ArrayList<>();
    sortOrder.add(D_SV_1);

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(_rawNoDictSegmentIndexDir,
        null, sortOrder)) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of rows returned by PinotSegmentRecordReader is incorrect");

    GenericRow prev = outputRows.get(0);
    for (int i = 1; i < outputRows.size(); i++) {
      GenericRow current = outputRows.get(i);
      Assert.assertTrue(((String) prev.getValue(D_SV_1)).compareTo((String) current.getValue(D_SV_1)) <= 0);
      prev = current;
    }
  }

  @Test
  public void testPinotSegmentRecordReaderForwardIndexOnly()
      throws Exception {
    List<GenericRow> outputRows = new ArrayList<>();

    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader();
    pinotSegmentRecordReader.init(_segmentIndexDir, null, null, false, true);
    try (pinotSegmentRecordReader) {
      while (pinotSegmentRecordReader.hasNext()) {
        outputRows.add(pinotSegmentRecordReader.next());
      }
    }

    Assert.assertEquals(outputRows.size(), _rows.size(),
        "Number of rows returned by PinotSegmentRecordReader with forwardIndexOnly is incorrect");
    for (int i = 0; i < outputRows.size(); i++) {
      GenericRow outputRow = outputRows.get(i);
      GenericRow row = _rows.get(i);
      Assert.assertEquals(outputRow.getValue(D_SV_1), row.getValue(D_SV_1));
      Assert.assertTrue(PinotSegmentUtil.compareMultiValueColumn(outputRow.getValue(D_MV_1), row.getValue(D_MV_1)));
      Assert.assertEquals(outputRow.getValue(M1), row.getValue(M1));
      Assert.assertEquals(outputRow.getValue(M2), row.getValue(M2));
      Assert.assertEquals(outputRow.getValue(TIME), row.getValue(TIME));
    }
  }

  @Test
  public void testCreationTimeDefaultsComparisonColumnForRawInput()
      throws Exception {
    long creationTime = 123_456_789L;
    String segmentName = "rawInputComparisonTime";
    GenericRow missingComparison = _rows.get(0).copy();
    missingComparison.putValue(PinotSegmentRecordReader.CREATION_TIME_COLUMN, -1L);
    GenericRow nullComparison = _rows.get(1).copy();
    nullComparison.putValue(COMPARISON_COLUMN, null);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(createComparisonTableConfig(), createComparisonSchema());
    config.setOutDir(_segmentOutputDir);
    config.setSegmentName(segmentName);
    config.setCreationTime(String.valueOf(creationTime));
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(missingComparison, nullComparison)));
    driver.build();

    assertComparisonTime(new File(_segmentOutputDir, segmentName), creationTime, creationTime);
  }

  @Test
  public void testCreationTimeFillsAndPreservesPhysicalComparisonColumnAcrossRewrites()
      throws Exception {
    SegmentMetadataImpl sourceMetadata = new SegmentMetadataImpl(_segmentIndexDir);
    long sourceCreationTime = sourceMetadata.getIndexCreationTime();
    Assert.assertNull(sourceMetadata.getColumnMetadataFor(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
    Assert.assertNull(sourceMetadata.getColumnMetadataFor(COMPARISON_COLUMN));

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(createTableConfig(), createPinotSchema());
    ImmutableSegment sourceSegment = ImmutableSegmentLoader.load(_segmentIndexDir, indexLoadingConfig);
    try {
      Assert.assertFalse(sourceSegment.getColumnNames().contains(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
      Assert.assertFalse(
          sourceSegment.getPhysicalColumnNames().contains(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
      Assert.assertNull(sourceSegment.getDataSourceNullable(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
    } finally {
      sourceSegment.destroy();
    }

    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(_segmentIndexDir, Set.of(D_SV_1), null);
      GenericRow row = recordReader.next();
      Assert.assertEquals(row.getValue(D_SV_1), _rows.get(0).getValue(D_SV_1));
      Assert.assertEquals(row.getValue(PinotSegmentRecordReader.CREATION_TIME_COLUMN), sourceCreationTime);
      Assert.assertFalse(
          row.getFieldToValueMap().containsKey(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
      Assert.assertNull(row.getValue(COMPARISON_COLUMN));
    }

    long firstArtifactCreationTime = sourceCreationTime + 1_000L;
    File firstRewrite = rewriteSegment(_segmentIndexDir, "comparisonTimeRewrite1", firstArtifactCreationTime);
    assertComparisonTime(firstRewrite, firstArtifactCreationTime, sourceCreationTime);

    long secondArtifactCreationTime = sourceCreationTime + 2_000L;
    File secondRewrite = rewriteSegment(firstRewrite, "comparisonTimeRewrite2", secondArtifactCreationTime);
    assertComparisonTime(secondRewrite, secondArtifactCreationTime, sourceCreationTime);
  }

  private File rewriteSegment(File sourceIndexDir, String segmentName, long artifactCreationTime)
      throws Exception {
    TableConfig tableConfig = createComparisonTableConfig();
    Schema schema = createComparisonSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(_segmentOutputDir);
    config.setSegmentName(segmentName);
    config.setCreationTime(String.valueOf(artifactCreationTime));

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment sourceSegment = ImmutableSegmentLoader.load(sourceIndexDir, indexLoadingConfig, false);
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(sourceSegment);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
    } finally {
      sourceSegment.destroy();
    }
    return new File(_segmentOutputDir, segmentName);
  }

  private void assertComparisonTime(File indexDir, long artifactCreationTime, long sourceCreationTime)
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(indexDir);
    Assert.assertEquals(metadata.getIndexCreationTime(), artifactCreationTime);
    Assert.assertNull(metadata.getColumnMetadataFor(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
    ColumnMetadata comparisonTimeMetadata = metadata.getColumnMetadataFor(COMPARISON_COLUMN);
    Assert.assertNotNull(comparisonTimeMetadata);
    Assert.assertEquals(comparisonTimeMetadata.getFieldType(), FieldType.DIMENSION);
    Assert.assertEquals(comparisonTimeMetadata.getDataType(), DataType.LONG);
    Assert.assertTrue(comparisonTimeMetadata.isSingleValue());
    Assert.assertEquals(comparisonTimeMetadata.getMinValue(), sourceCreationTime);
    Assert.assertEquals(comparisonTimeMetadata.getMaxValue(), sourceCreationTime);

    IndexLoadingConfig indexLoadingConfig =
        new IndexLoadingConfig(createComparisonTableConfig(), createComparisonSchema());
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig);
    try {
      Assert.assertFalse(segment.getColumnNames().contains(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
      Assert.assertFalse(segment.getPhysicalColumnNames().contains(PinotSegmentRecordReader.CREATION_TIME_COLUMN));
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(COMPARISON_COLUMN));
      Assert.assertEquals(segment.getValue(0, COMPARISON_COLUMN), sourceCreationTime);
    } finally {
      segment.destroy();
    }
  }

  private Schema createComparisonSchema() {
    Schema schema = createPinotSchema();
    schema.addField(new DimensionFieldSpec(COMPARISON_COLUMN, DataType.LONG, true, Long.MIN_VALUE));
    return schema;
  }

  private TableConfig createComparisonTableConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(
        new TransformConfig(COMPARISON_COLUMN, PinotSegmentRecordReader.CREATION_TIME_COLUMN)));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME)
        .setIngestionConfig(ingestionConfig).build();
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(_segmentOutputDir));
  }
}
