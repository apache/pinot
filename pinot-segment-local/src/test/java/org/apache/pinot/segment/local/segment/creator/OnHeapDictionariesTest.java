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
package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for On-Heap dictionary implementations.
 */
public class OnHeapDictionariesTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapDictionariesTest.class);

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "onHeapDict";
  private static final String SEGMENT_NAME = "onHeapDict";

  private static final long RANDOM_SEED = System.nanoTime();
  private static final int NUM_ROWS = 1;
  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";

  private IndexSegment _offHeapSegment;
  private IndexSegment _onHeapSegment;

  @BeforeClass
  public void setup()
      throws Exception {
    Schema schema = buildSchema();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();
    buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, tableConfig, schema);

    IndexLoadingConfig loadingConfig = new IndexLoadingConfig();
    loadingConfig.setReadMode(ReadMode.mmap);
    loadingConfig.setSegmentVersion(SegmentVersion.v3);
    _offHeapSegment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), loadingConfig);

    loadingConfig.setOnHeapDictionaryColumns(new HashSet<>(
        Arrays.asList(new String[]{INT_COLUMN, LONG_COLUMN, FLOAT_COLUMN, DOUBLE_COLUMN, STRING_COLUMN})));

    _onHeapSegment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), loadingConfig);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  /**
   * This test compares the on-heap and off-heap loaded dictionaries for the same segment.
   */
  @Test
  public void test() {
    testColumn(INT_COLUMN);
    testColumn(LONG_COLUMN);
    testColumn(FLOAT_COLUMN);
    testColumn(DOUBLE_COLUMN);
    testColumn(STRING_COLUMN);
  }

  private void testColumn(String column) {
    DataSource onHeapDataSource = _onHeapSegment.getDataSource(column);
    DataSource offHeapDataSource = _offHeapSegment.getDataSource(column);

    FieldSpec.DataType onHeapDataType = onHeapDataSource.getDataSourceMetadata().getDataType();
    FieldSpec.DataType offHeapDataType = offHeapDataSource.getDataSourceMetadata().getDataType();
    Assert.assertEquals(onHeapDataType, offHeapDataType);

    Dictionary onHeapDict = onHeapDataSource.getDictionary();
    Dictionary offHeapDict = offHeapDataSource.getDictionary();

    Assert.assertEquals(onHeapDict.length(), offHeapDict.length());

    for (int dictId = 0; dictId < onHeapDict.length(); dictId++) {
      Assert.assertEquals(onHeapDict.get(dictId), offHeapDict.get(dictId));
      Assert.assertEquals(onHeapDict.getStringValue(dictId), offHeapDict.getStringValue(dictId));

      switch (onHeapDataType) {
        case INT:
          Assert.assertEquals(onHeapDict.getIntValue(dictId), offHeapDict.getIntValue(dictId));
          Assert.assertEquals(onHeapDict.getLongValue(dictId), offHeapDict.getLongValue(dictId));
          Assert.assertEquals(onHeapDict.getFloatValue(dictId), offHeapDict.getFloatValue(dictId));
          Assert.assertEquals(onHeapDict.getDoubleValue(dictId), offHeapDict.getDoubleValue(dictId));
          break;

        case LONG:
          Assert.assertEquals(onHeapDict.getLongValue(dictId), offHeapDict.getLongValue(dictId));
          break;

        case FLOAT:
          Assert.assertEquals(onHeapDict.getFloatValue(dictId), offHeapDict.getFloatValue(dictId));
          Assert.assertEquals(onHeapDict.getDoubleValue(dictId), offHeapDict.getDoubleValue(dictId));
          break;

        case DOUBLE:
          Assert.assertEquals(onHeapDict.getDoubleValue(dictId), offHeapDict.getDoubleValue(dictId));
          break;

        case STRING:
          Assert.assertEquals(onHeapDict.getStringValue(dictId), offHeapDict.getStringValue(dictId));
          break;

        default:
          throw new RuntimeException("Unexpected data type for dictionary for column: " + column);
      }
    }
  }

  /**
   * Helper method to build a segment with random data as per the schema.
   *
   * @param segmentDirName Name of segment directory
   * @param segmentName Name of segment
   * @param schema Schema for segment
   * @return Schema built for the segment
   * @throws Exception
   */
  private Schema buildSegment(String segmentDirName, String segmentName, TableConfig tableConfig, Schema schema)
      throws Exception {

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);

    Random random = new Random(RANDOM_SEED);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);

    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      map.put(INT_COLUMN, random.nextInt());
      map.put(LONG_COLUMN, random.nextLong());
      map.put(FLOAT_COLUMN, random.nextFloat());
      map.put(DOUBLE_COLUMN, random.nextDouble());
      map.put(STRING_COLUMN, RandomStringUtils.randomAscii(100));

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return schema;
  }

  /**
   * Helper method to build a schema with provided number of metric columns.
   *
   * @return Schema containing the given number of metric columns
   */
  private static Schema buildSchema() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(INT_COLUMN, FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec(LONG_COLUMN, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(FLOAT_COLUMN, FieldSpec.DataType.FLOAT, true));
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true));
    return schema;
  }
}
