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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnStatistics;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.BoundedColumnValuePartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment.SEGMENT_PADDING_CHARACTER;
import static org.testng.Assert.*;


public class ColumnMetadataTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ColumnMetadataTest");
  private static final String CREATOR_VERSION = "TestHadoopJar.1.1.1";
  private static final String RAW_TABLE_NAME = "testTable";

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  public SegmentGeneratorConfig createSegmentConfigWithoutCreator() {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4", "column7"))
        .build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("column3", DataType.STRING)
        .addSingleValueDimension("column4", DataType.STRING)
        .addMultiValueDimension("column6", DataType.INT)
        .addMultiValueDimension("column7", DataType.INT)
        .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|HOURS", "1:HOURS")
        .build();
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(filePath), FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME,
            tableConfig, schema);
    config.setSegmentNamePostfix("1");
    return config;
  }

  public SegmentGeneratorConfig createSegmentConfigWithCreator() {
    SegmentGeneratorConfig config = createSegmentConfigWithoutCreator();
    config.setCreatorVersion(CREATOR_VERSION);
    return config;
  }

  public void verifySegmentAfterLoading(SegmentMetadata segmentMetadata) {
    // Single-value dictionary-encoded string dimension column
    ColumnMetadata col3Meta = segmentMetadata.getColumnMetadataFor("column3");
    assertEquals(col3Meta.getFieldSpec(),
        new DimensionFieldSpec("column3", DataType.STRING, true, FieldSpec.DEFAULT_MAX_LENGTH, null));
    assertEquals(col3Meta.getCardinality(), 5);
    assertEquals(col3Meta.getTotalDocs(), 100000);
    assertEquals(col3Meta.getBitsPerElement(), 3);
    assertEquals(col3Meta.getColumnMaxLength(), 4);
    assertFalse(col3Meta.isSorted());
    assertTrue(col3Meta.hasDictionary());
    assertEquals(col3Meta.getMaxNumberOfMultiValues(), 0);
    assertEquals(col3Meta.getTotalNumberOfEntries(), 100000);
    assertFalse(col3Meta.isAutoGenerated());

    // Single-value raw string dimension column
    ColumnMetadata col4Meta = segmentMetadata.getColumnMetadataFor("column4");
    assertEquals(col4Meta.getFieldSpec(),
        new DimensionFieldSpec("column4", DataType.STRING, true, FieldSpec.DEFAULT_MAX_LENGTH, null));
    assertEquals(col4Meta.getCardinality(), 5);
    assertEquals(col4Meta.getTotalDocs(), 100000);
    assertEquals(col4Meta.getBitsPerElement(), 3);
    assertEquals(col4Meta.getColumnMaxLength(), 0);
    assertFalse(col4Meta.isSorted());
    assertFalse(col4Meta.hasDictionary());
    assertEquals(col4Meta.getMaxNumberOfMultiValues(), 0);
    assertEquals(col4Meta.getTotalNumberOfEntries(), 100000);
    assertFalse(col4Meta.isAutoGenerated());

    // Multi-value dictionary-encoded int dimension column
    ColumnMetadata col6Meta = segmentMetadata.getColumnMetadataFor("column6");
    assertEquals(col6Meta.getFieldSpec(), new DimensionFieldSpec("column6", DataType.INT, false));
    assertEquals(col6Meta.getCardinality(), 18499);
    assertEquals(col6Meta.getTotalDocs(), 100000);
    assertEquals(col6Meta.getBitsPerElement(), 15);
    assertEquals(col6Meta.getColumnMaxLength(), 0);
    assertFalse(col6Meta.isSorted());
    assertTrue(col6Meta.hasDictionary());
    assertEquals(col6Meta.getMaxNumberOfMultiValues(), 13);
    assertEquals(col6Meta.getTotalNumberOfEntries(), 106688);
    assertFalse(col6Meta.isAutoGenerated());

    // Multi-value raw int dimension column
    ColumnMetadata col7Meta = segmentMetadata.getColumnMetadataFor("column7");
    assertEquals(col7Meta.getFieldSpec(), new DimensionFieldSpec("column7", DataType.INT, false));
    assertEquals(col7Meta.getCardinality(), 359);
    assertEquals(col7Meta.getTotalDocs(), 100000);
    assertEquals(col7Meta.getBitsPerElement(), 9);
    assertEquals(col7Meta.getColumnMaxLength(), 0);
    assertFalse(col7Meta.isSorted());
    assertFalse(col7Meta.hasDictionary());
    assertEquals(col7Meta.getMaxNumberOfMultiValues(), 24);
    assertEquals(col7Meta.getTotalNumberOfEntries(), 134090);
    assertFalse(col7Meta.isAutoGenerated());

    // Date-time column
    ColumnMetadata timeColumn = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    assertEquals(timeColumn.getFieldSpec(),
        new DateTimeFieldSpec("daysSinceEpoch", DataType.INT, "EPOCH|HOURS", "1:HOURS"));
    assertEquals(timeColumn.getColumnName(), "daysSinceEpoch");
    assertEquals(timeColumn.getCardinality(), 1);
    assertEquals(timeColumn.getTotalDocs(), 100000);
    assertEquals(timeColumn.getBitsPerElement(), 1);
    assertEquals(timeColumn.getColumnMaxLength(), 0);
    assertTrue(timeColumn.isSorted());
    assertTrue(timeColumn.hasDictionary());
    assertEquals(timeColumn.getMaxNumberOfMultiValues(), 0);
    assertEquals(timeColumn.getTotalNumberOfEntries(), 100000);
    assertFalse(timeColumn.isAutoGenerated());
  }

  @Test
  public void testAllFieldsInitialized()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = createSegmentConfigWithCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(INDEX_DIR.listFiles()[0]);
    verifySegmentAfterLoading(segmentMetadata);

    // Make sure we got the creator name as well.
    String creatorName = segmentMetadata.getCreatorName();
    assertEquals(creatorName, CREATOR_VERSION);
  }

  @Test
  public void testAllFieldsExceptCreatorName()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = createSegmentConfigWithoutCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(INDEX_DIR.listFiles()[0]);
    verifySegmentAfterLoading(segmentMetadata);

    // Make sure we get null for creator name.
    assertNull(segmentMetadata.getCreatorName());
  }

  @Test
  public void testPaddingCharacter()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = createSegmentConfigWithoutCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(INDEX_DIR.listFiles()[0]);
    verifySegmentAfterLoading(segmentMetadata);
  }

  @Test
  public void testSegmentPartitionedWithBoundedColumnValue()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = createSegmentConfigWithoutCreator();
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("columnValues", "P,w,L");
    functionConfig.put("columnValuesDelimiter", ",");
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap("column3", new ColumnPartitionConfig("BoundedColumnValue", 4, functionConfig)));
    config.setSegmentPartitionConfig(segmentPartitionConfig);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(INDEX_DIR.listFiles()[0]);
    verifySegmentAfterLoading(segmentMetadata);
    // Make sure we get null for creator name.
    assertNull(segmentMetadata.getCreatorName());

    // Verify segment partitioning metadata.
    ColumnMetadata col3Meta = segmentMetadata.getColumnMetadataFor("column3");
    assertNotNull(col3Meta.getPartitionFunction());
    assertTrue(col3Meta.getPartitionFunction() instanceof BoundedColumnValuePartitionFunction);
    assertEquals(col3Meta.getPartitionFunction().getNumPartitions(), 4);
    assertEquals(col3Meta.getPartitionFunction().getFunctionConfig(), functionConfig);
    assertEquals(col3Meta.getPartitions(), Stream.of(0, 1, 2, 3).collect(Collectors.toSet()));
  }

  @Test
  public void testMetadataWithEscapedValue()
      throws ConfigurationException {
    // Reading metadata file:
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("data/metadata-with-unescaped.properties");
    File metadataFile = new File(resource.getFile());
    PropertiesConfiguration propertiesConfiguration = CommonsConfigurationUtils.fromFile(metadataFile);
    ColumnMetadataImpl installationOutput =
        ColumnMetadataImpl.fromPropertiesConfiguration("installation_output", propertiesConfiguration);
    assertEquals(installationOutput.getMinValue(),
        "\r\n\r\n  utils   em::C:\\dir\\utils\r\nPSParentPath            : Mi");
  }

  @Test
  public void testComplexFieldSpec() {
    ComplexFieldSpec intMapFieldSpec = new ComplexFieldSpec("intMap", DataType.MAP, true,
        Map.of("key", new DimensionFieldSpec("key", DataType.STRING, true), "value",
            new DimensionFieldSpec("value", DataType.INT, true)));
    ColumnIndexCreationInfo columnIndexCreationInfo =
        new ColumnIndexCreationInfo(new DefaultColumnStatistics(null, null, null, false, 1, 1), false, false, false,
            Map.of());
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    SegmentColumnarIndexCreator.addColumnMetadataInfo(config, "intMap", columnIndexCreationInfo, 1, intMapFieldSpec,
        false, -1);
    ColumnMetadataImpl intMapColumnMetadata = ColumnMetadataImpl.fromPropertiesConfiguration("intMap", config);
    assertEquals(intMapColumnMetadata.getFieldSpec(), intMapFieldSpec);
  }

  @Test
  public void testSetAndCheckIndexSizes() {
    ColumnMetadataImpl meta = new ColumnMetadataImpl.Builder().build();
    meta.addIndexSize(IndexService.getInstance().getNumericId(StandardIndexes.json()), 12345L);
    meta.addIndexSize(IndexService.getInstance().getNumericId(StandardIndexes.h3()), 0xffffffffffffL);
    meta.addIndexSize(IndexService.getInstance().getNumericId(StandardIndexes.vector()), 0);

    assertEquals(meta.getNumIndexes(), 3);
    assertEquals(meta.getIndexSizeFor(StandardIndexes.json()), 12345L);
    assertEquals(meta.getIndexSizeFor(StandardIndexes.h3()), 0xffffffffffffL);
    assertEquals(meta.getIndexSizeFor(StandardIndexes.vector()), 0);
    assertEquals(meta.getIndexSizeFor(StandardIndexes.inverted()), ColumnMetadata.INDEX_NOT_FOUND);

    try {
      meta.addIndexSize(IndexService.getInstance().getNumericId(StandardIndexes.fst()), -1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Index size should be a non-negative integer value between 0 and 281474976710655");
    }
  }

  @Test
  public void testBadTimeColumnWithoutContinueOnError()
      throws Exception {
    SegmentGeneratorConfig config = createSegmentConfigWithCreator();
    // column4 is not a time column and should cause an exception to be thrown when the segment is sealed and time
    // metadata is being parsed and written
    config.setTimeColumnName("column4");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    assertThrows(NumberFormatException.class, driver::build);
  }

  @Test
  public void testBadTimeColumnWithContinueOnError()
      throws Exception {
    SegmentGeneratorConfig config = createSegmentConfigWithCreator();

    // column4 is not a time column and should cause an exception to be thrown when the segment is sealed and time
    // metadata is being parsed and written
    config.setTimeColumnName("column4");
    config.setContinueOnError(true);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(INDEX_DIR.listFiles()[0]);
    assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(segmentMetadata.getStartTime(), TimeUtils.getValidMinTimeMillis());
    assertTrue(System.currentTimeMillis() - segmentMetadata.getEndTime() < 60_000L);
  }
}
