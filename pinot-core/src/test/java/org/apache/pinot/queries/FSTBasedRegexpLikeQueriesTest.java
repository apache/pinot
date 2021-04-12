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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
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


public class FSTBasedRegexpLikeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
  private static final String URL_COL = "URL_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";
  private static final Integer INT_BASE_VALUE = 1000;
  private static final Integer NUM_ROWS = 1024;

  private final List<GenericRow> _rows = new ArrayList<>();

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

    buildSegment();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> fstIndexCols = new HashSet<>();
    fstIndexCols.add(DOMAIN_NAMES_COL);
    indexLoadingConfig.setFSTIndexColumns(fstIndexCols);

    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<String> getURLSufficies() {
    return Arrays.asList("/a", "/b", "/c", "/d");
  }

  private List<String> getNoIndexData() {
    return Arrays.asList("test1", "test2", "test3", "test4", "test5");
  }

  private List<String> getDomainNames() {
    return Arrays
        .asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd", "www.sd.domain1.com",
            "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
            "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
            "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private List<GenericRow> createTestData(int numRows)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    List<String> urlSufficies = getURLSufficies();
    List<String> noIndexData = getNoIndexData();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      String url = domain + urlSufficies.get(i % urlSufficies.size());

      GenericRow row = new GenericRow();
      row.putField(INT_COL_NAME, INT_BASE_VALUE + i);
      row.putField(NO_INDEX_STRING_COL_NAME, noIndexData.get(i % noIndexData.size()));
      row.putField(DOMAIN_NAMES_COL, domain);
      row.putField(URL_COL, url);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs
        .add(new FieldConfig(DOMAIN_NAMES_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null));
    fieldConfigs.add(new FieldConfig(URL_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(URL_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void testInterSegmentSelectionQueryHelper(String query, int expectedResultSize,
      List<Serializable[]> expectedResults) {
    // PQL
    BrokerResponseNative brokerResponseNative = getBrokerResponseForPqlQuery(query);
    SelectionResults selectionResults = brokerResponseNative.getSelectionResults();
    List<String> columns = selectionResults.getColumns();
    Assert.assertEquals(columns.size(), 2);
    List<Serializable[]> rows = selectionResults.getRows();
    Assert.assertEquals(rows.size(), expectedResultSize);

    if (expectedResults != null) {
      for (int i = 0; i < expectedResults.size(); i++) {
        Serializable[] actualRow = rows.get(i);
        Serializable[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow[0], String.valueOf(expectedRow[0]));
        Assert.assertEquals(actualRow[1], expectedRow[1]);
      }
    }

    // SQL
    brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), 2);
    Assert.assertEquals(dataSchema.getColumnName(0), "INT_COL");
    Assert.assertEquals(dataSchema.getColumnName(1), "URL_COL");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    List<Object[]> results = resultTable.getRows();
    Assert.assertEquals(results.size(), expectedResultSize);

    if (expectedResults != null) {
      for (int i = 0; i < expectedResults.size(); i++) {
        Object[] actualRow = results.get(i);
        Serializable[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow, expectedRow);
      }
    }
  }

  private void testSelectionResults(String query, int expectedResultSize, List<Serializable[]> expectedResults)
      throws Exception {
    Operator<IntermediateResultsBlock> operator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> resultset = (List<Object[]>) operatorResult.getSelectionResult();
    Assert.assertNotNull(resultset);
    Assert.assertEquals(resultset.size(), expectedResultSize);
    if (expectedResults != null) {
      for (int i = 0; i < expectedResultSize; i++) {
        Object[] actualRow = resultset.get(i);
        Object[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow.length, expectedRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          Object actualColValue = actualRow[j];
          Object expectedColValue = expectedRow[j];
          Assert.assertEquals(actualColValue, expectedColValue);
        }
      }
    }
  }

  private AggregationGroupByResult getGroupByResults(String query)
      throws Exception {
    AggregationGroupByOperator operator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = operator.nextBlock();
    return resultsBlock.getAggregationGroupByResult();
  }

  private void matchGroupResult(AggregationGroupByResult result, String key, long count) {
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = result.getGroupKeyIterator();
    boolean found = false;
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      if (groupKey._keys[0].equals(key)) {
        Assert.assertEquals(((Number) result.getResultForGroupId(0, groupKey._groupId)).longValue(), count);
        found = true;
      }
    }
    Assert.assertTrue(found);
  }

  private void testInterSegmentAggregationQueryHelper(String query, long expectedCount) {
    // PQL
    BrokerResponseNative brokerResponseNative = getBrokerResponseForPqlQuery(query);
    List<AggregationResult> aggregationResults = brokerResponseNative.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 1);
    Assert.assertEquals(aggregationResults.get(0).getValue().toString(), String.valueOf(expectedCount));

    // SQL
    brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), 1);
    Assert.assertEquals(dataSchema.getColumnName(0), "count(*)");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    Assert.assertEquals(row.length, 1);
    Assert.assertEquals(row[0], expectedCount);
  }

  @Test
  public void testFSTBasedRegexLike()
      throws Exception {
    // Select queries on col with FST + inverted index.
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') LIMIT 50000";
    testSelectionResults(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*') LIMIT 50000";
    testSelectionResults(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*') LIMIT 50000";
    testSelectionResults(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*') LIMIT 50000";
    testSelectionResults(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*com') LIMIT 50000";
    testSelectionResults(query, 256, null);

    // Select queries on col with just FST index.
    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 50000";
    testSelectionResults(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 5";
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Serializable[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Serializable[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Serializable[]{1016, "www.domain1.com/a"});
    testSelectionResults(query, 5, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.sd.domain1.*') LIMIT 50000";
    testSelectionResults(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain1.*') LIMIT 50000";
    testSelectionResults(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 50000";
    testSelectionResults(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Serializable[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Serializable[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Serializable[]{1004, "www.sd.domain1.com/a"});
    testSelectionResults(query, 5, expected);
    testSelectionResults(query, 5, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 50000";
    testSelectionResults(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1004, "www.sd.domain1.com/b"});
    expected.add(new Serializable[]{1008, "www.domain2.com/c"});
    expected.add(new Serializable[]{1012, "www.sd.domain2.co.cd/d"});
    expected.add(new Serializable[]{1016, "www.domain1.com/a"});
    testSelectionResults(query, 5, null);
  }

  @Test
  public void testFSTBasedRegexpLikeWithOtherFilters()
      throws Exception {
    String query;

    // Select queries on columns with combination of FST Index , (FST + Inverted Index), No index and other constraints.
    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testSelectionResults(query, 52, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testSelectionResults(query, 51, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testSelectionResults(query, 13, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testSelectionResults(query, 0, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testSelectionResults(query, 12, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') and INT_COL=1000 LIMIT 50000";
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    testSelectionResults(query, 1, expected);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test2') and INT_COL=1001 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    testSelectionResults(query, 1, expected);
  }

  @Test
  public void testGroupByOnFSTBasedRegexpLike()
      throws Exception {
    String query;
    query =
        "SELECT DOMAIN_NAMES, count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') group by DOMAIN_NAMES LIMIT 50000";
    AggregationGroupByResult result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com", 64);
    matchGroupResult(result, "www.domain1.co.ab", 64);
    matchGroupResult(result, "www.domain1.co.bc", 64);
    matchGroupResult(result, "www.domain1.co.cd", 64);

    query =
        "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') group by URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 13);
    matchGroupResult(result, "www.sd.domain1.com/a", 13);
    matchGroupResult(result, "www.domain2.com/a", 13);
    matchGroupResult(result, "www.sd.domain2.com/a", 13);

    query =
        "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') group by URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 13);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query =
        "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL > 1005 group by URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query =
        "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a') group by URL_COL LIMIT 50000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 64);
  }

  @Test
  public void testInterSegment() {
    String query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test2') and INT_COL=1001 LIMIT 50000";
    List<Serializable[]> expected = new ArrayList<>();
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Serializable[]{1001, "www.domain1.co.ab/b"});
    testInterSegmentSelectionQueryHelper(query, 4, expected);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') and INT_COL=1000 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    expected.add(new Serializable[]{1000, "www.domain1.com/a"});
    testInterSegmentSelectionQueryHelper(query, 4, expected);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') ORDER  BY INT_COL LIMIT 5000";
    testInterSegmentSelectionQueryHelper(query, 48, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentSelectionQueryHelper(query, 0, null);

    query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentSelectionQueryHelper(query, 52, null);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a')";
    testInterSegmentAggregationQueryHelper(query, 256);

    query =
        "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL > 1005 ";
    testInterSegmentAggregationQueryHelper(query, 200);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') and REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentAggregationQueryHelper(query, 204);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') and REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentAggregationQueryHelper(query, 208);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*')";
    testInterSegmentAggregationQueryHelper(query, 1024);
  }
}
