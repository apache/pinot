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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FilteredAggregationsTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";
  private static final Integer INT_BASE_VALUE = 0;
  private static final Integer NUM_ROWS = 5;


  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment(FIRST_SEGMENT_NAME);
    buildSegment(SECOND_SEGMENT_NAME);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();

    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(INT_COL_NAME);

    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    ImmutableSegment firstImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, FIRST_SEGMENT_NAME), indexLoadingConfig);
    ImmutableSegment secondImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SECOND_SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = firstImmutableSegment;
    _indexSegments = Arrays.asList(firstImmutableSegment, secondImmutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<String> getNoIndexData() {
    return Arrays.asList("test1", "test2", "test3", "test4", "test5");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();

    List<String> noIndexData = getNoIndexData();
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putField(INT_COL_NAME, INT_BASE_VALUE + i);
      row.putField(NO_INDEX_STRING_COL_NAME, noIndexData.get(i % noIndexData.size()));

      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(INT_COL_NAME)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void testInterSegmentAggregationQueryHelper(String query, long[] expectedValue,
      int numElements) {
    // SQL
    BrokerResponseNative brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), numElements);
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    Assert.assertEquals(row.length, numElements);

    for (int i = 0; i < numElements; i++) {
      Assert.assertEquals(row[i], expectedValue[i]);
    }
  }

  private void testInterSegmentAggregationQueryHelper(String firstQuery, String secondQuery) {
    // SQL
    BrokerResponseNative firstBrokerResponseNative = getBrokerResponseForSqlQuery(firstQuery);
    BrokerResponseNative secondBrokerResponseNative = getBrokerResponseForSqlQuery(secondQuery);
    ResultTable firstResultTable = firstBrokerResponseNative.getResultTable();
    ResultTable secondResultTable = secondBrokerResponseNative.getResultTable();
    DataSchema firstDataSchema = firstResultTable.getDataSchema();
    DataSchema secondDataSchema = secondResultTable.getDataSchema();

    Assert.assertEquals(firstDataSchema.size(), secondDataSchema.size());

    List<Object[]> firstSetOfRows = firstResultTable.getRows();
    List<Object[]> secondSetOfRows = secondResultTable.getRows();

    Assert.assertEquals(firstSetOfRows.size(), secondSetOfRows.size());

    for (int i = 0; i < firstSetOfRows.size(); i++) {
      Object[] firstSetRow = firstSetOfRows.get(i);
      Object[] secondSetRow = secondSetOfRows.get(i);

      Assert.assertEquals(firstSetRow.length, secondSetRow.length);

      for (int j = 0; j < firstSetRow.length; j++) {
        Assert.assertEquals(firstSetRow[j], secondSetRow[j]);
      }
    }
  }

  @Test
  public void testInterSegment() {
    /*String query =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL < 3)"
            + "FROM MyTable WHERE INT_COL > 1";
    String nonFilterQuery =
        "SELECT SUM(INT_COL)"
            + "FROM MyTable WHERE INT_COL > 1 AND INT_COL < 3";

    testInterSegmentAggregationQueryHelper(query, nonFilterQuery);

    query =
        "SELECT COUNT(*) FILTER(WHERE INT_COL = 4)"
            + "FROM MyTable";
    nonFilterQuery =
        "SELECT COUNT(*)"
            + "FROM MyTable WHERE INT_COL = 4";

    testInterSegmentAggregationQueryHelper(query, nonFilterQuery);*/

    /*String query =
        "SELECT count(*) FILTER(WHERE INT_COL > 3),"
            + "SUM(INT_COL) FILTER(WHERE REGEXP_LIKE(NO_INDEX_COL, 'test1'))"
            + "FROM MyTable";*/

    String query =
        "SELECT SUM(*)"
            + "FROM MyTable WHERE REGEXP_LIKE(NO_INDEX_COL, 'test1')";

    testInterSegmentAggregationQueryHelper(query, new long[] {92}, 1);

    query =
        "SELECT count(*)"
            + "FROM MyTable WHERE NO_INDEX_COL IS NULL";

    testInterSegmentAggregationQueryHelper(query, new long[] {92}, 1);
  }
}
