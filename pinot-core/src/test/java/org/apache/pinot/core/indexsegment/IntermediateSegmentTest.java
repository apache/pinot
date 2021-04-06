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
package org.apache.pinot.core.indexsegment;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.readers.IntermediateSegmentRecordReader;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.indexsegment.mutable.IntermediateSegment;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class IntermediateSegmentTest {
  private static final String AVRO_DATA_SV = "data/test_data-sv.avro";
  private static final String AVRO_DATA_MV = "data/test_data-mv.avro";
  private static final String SEGMENT_NAME = "testSegmentName";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "IntermediateSegmentTest");

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @DataProvider(name = "segmentCreationTestCases")
  private static Object[][] createSegmentCreationTestCases() {
    return new Object[][]{{AVRO_DATA_SV}, {AVRO_DATA_MV}};
  }

  @Test(dataProvider = "segmentCreationTestCases")
  public void testOfflineSegmentCreationFromDifferentWays(String inputFile)
      throws Exception {
    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(inputFile);
    assertNotNull(resource);
    String filePath = resource.getFile();

    Schema schema = createSchema(inputFile);
    TableConfig tableConfig = createTableConfig(inputFile);

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Arrays.asList("column6", "column7"));

    IndexSegment segmentFromIntermediateSegment = buildSegmentFromIntermediateSegment(segmentGeneratorConfig);
    IndexSegment segmentFromAvroRecordReader = buildSegmentFromAvroRecordReader(segmentGeneratorConfig);

    assertNotNull(segmentFromIntermediateSegment);
    assertNotNull(segmentFromAvroRecordReader);
    assertEquals(segmentFromIntermediateSegment.getColumnNames(), segmentFromAvroRecordReader.getColumnNames());
    Set<String> physicalColumnsFromIntermediateSegment = segmentFromIntermediateSegment.getPhysicalColumnNames();
    Set<String> physicalColumnsFromAvroSegment = segmentFromAvroRecordReader.getPhysicalColumnNames();
    assertEquals(physicalColumnsFromIntermediateSegment, physicalColumnsFromAvroSegment);

    // Comparison for every columns
    for (String column : physicalColumnsFromIntermediateSegment) {
      DataSource dataSourceFromIntermediateSegment = segmentFromIntermediateSegment.getDataSource(column);
      DataSource dataSourceFromAvroRecordReader = segmentFromAvroRecordReader.getDataSource(column);

      // Comparison for dictionaries.
      Dictionary actualDictionary = dataSourceFromIntermediateSegment.getDictionary();
      Dictionary expectedDictionary = dataSourceFromAvroRecordReader.getDictionary();
      assertEquals(actualDictionary.getMinVal(), expectedDictionary.getMinVal());
      assertEquals(actualDictionary.getMaxVal(), expectedDictionary.getMaxVal());
      assertEquals(actualDictionary.getValueType(), expectedDictionary.getValueType());
      assertEquals(actualDictionary.length(), expectedDictionary.length());
      int dictionaryLength = actualDictionary.length();
      for (int i = 0; i < dictionaryLength; i++) {
        assertEquals(actualDictionary.get(i), expectedDictionary.get(i));
      }

      // Comparison for inverted index
      InvertedIndexReader actualInvertedIndexReader = dataSourceFromIntermediateSegment.getInvertedIndex();
      InvertedIndexReader expectedInvertedIndexReader = dataSourceFromAvroRecordReader.getInvertedIndex();
      if (actualInvertedIndexReader != null) {
        for (int j = 0; j < dictionaryLength; j++) {
          assertEquals(actualInvertedIndexReader.getDocIds(j), expectedInvertedIndexReader.getDocIds(j));
        }
      }
    }
  }

  private IndexSegment buildSegmentFromIntermediateSegment(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    // Set intermediate segment record reader.
    String segmentName = SEGMENT_NAME + "_from_intermediate_segment";
    segmentGeneratorConfig.setSegmentName(segmentName);
    IntermediateSegment intermediateSegment = new IntermediateSegment(segmentGeneratorConfig);

    // Ingest data.
    ingestDataToIntermediateSegment(segmentGeneratorConfig, intermediateSegment);
    IntermediateSegmentRecordReader intermediateSegmentRecordReader =
        new IntermediateSegmentRecordReader(intermediateSegment);

    // Build the segment from intermediate segment.
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, intermediateSegmentRecordReader);
    driver.build();

    // Destroy intermediate segment after the segment creation.
    intermediateSegment.destroy();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.heap);
  }

  private IndexSegment buildSegmentFromAvroRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    // Use avro record reader by default
    String segmentName = SEGMENT_NAME + "_from_avro_reader";
    segmentGeneratorConfig.setSegmentName(segmentName);

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.heap);
  }

  private void ingestDataToIntermediateSegment(SegmentGeneratorConfig segmentGeneratorConfig,
      IntermediateSegment intermediateSegment)
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(new File(segmentGeneratorConfig.getInputFilePath()), null, null);

    GenericRow genericRow = new GenericRow();
    while (avroRecordReader.hasNext()) {
      genericRow.clear();
      genericRow = avroRecordReader.next(genericRow);
      intermediateSegment.index(genericRow, null);
    }
  }

  private static Schema createSchema(String inputFile)
      throws IOException {
    Schema schema;
    if (AVRO_DATA_SV.equals(inputFile)) {
      schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
          .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column6", FieldSpec.DataType.INT)
          .addSingleValueDimension("column7", FieldSpec.DataType.INT)
          .addSingleValueDimension("column9", FieldSpec.DataType.INT)
          .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
          .addMetric("column18", FieldSpec.DataType.INT)
          .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
    } else {
      URL resource = IntermediateSegmentTest.class.getClassLoader().getResource(inputFile);
      assertNotNull(resource);
      String filePath = resource.getFile();
      schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(new File(filePath));
    }
    return schema;
  }

  private static TableConfig createTableConfig(String inputFile) {
    TableConfig tableConfig;
    if (AVRO_DATA_SV.equals(inputFile)) {
      tableConfig =
          new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
              .setInvertedIndexColumns(Arrays.asList("column6", "column7", "column11", "column17", "column18")).build();
    } else {
      tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    }
    return tableConfig;
  }
}
