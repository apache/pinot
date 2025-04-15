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
package org.apache.pinot.queries;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

import static org.testng.Assert.assertNotNull;


/**
 * The <code>BaseMultiValueQueriesTest</code> class sets up the index segment for the multi-value queries test.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue
 *   <li>column1, METRIC, INT, 51594, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, F, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, T, T</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F</li>
 * </ul>
 */
public abstract class BaseMultiValueQueriesTest extends BaseQueriesTest {
  protected static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MultiValueQueriesTest");
  protected static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
  protected static final String RAW_TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testTable_1756015683_1756015683";

  //@formatter:off
  protected static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addMetric("column1", DataType.INT)
      .addMetric("column2", DataType.INT)
      .addSingleValueDimension("column3", DataType.STRING)
      .addSingleValueDimension("column5", DataType.STRING)
      .addMultiValueDimension("column6", DataType.INT)
      .addMultiValueDimension("column7", DataType.INT)
      .addSingleValueDimension("column8", DataType.INT)
      .addMetric("column9", DataType.INT)
      .addMetric("column10", DataType.INT)
      .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|DAYS", "1:DAYS")
      .build();
  protected static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setTimeColumnName("daysSinceEpoch")
      .setNoDictionaryColumns(List.of("column5"))
      .setInvertedIndexColumns(List.of("column3", "column7", "column8", "column9"))
      .build();
  static {
    // The segment generation code in SegmentColumnarIndexCreator will throw exception if start and end time in time
    // column are not in acceptable range. For this test, we first need to fix the input avro data to have the time
    // column values in allowed range. Until then, the check is explicitly disabled.
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setSegmentTimeValueCheck(false);
    ingestionConfig.setRowTimeValueCheck(false);
    TABLE_CONFIG.setIngestionConfig(ingestionConfig);
  }
  protected static final String FILTER =
      " WHERE column1 > 100000000"
      + " AND column2 BETWEEN 20000000 AND 1000000000"
      + " AND column3 <> 'w'"
      + " AND (column6 < 500000 OR column7 NOT IN (225, 407))"
      + " AND daysSinceEpoch = 1756015683";
  //@formatter:on

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments.
  private List<IndexSegment> _indexSegments;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  protected String getFilter() {
    return FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }
}
