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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class FSTBasedRegexpLikeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), FSTBasedRegexpLikeQueriesTest.class.getSimpleName());
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
  private static final String URL_COL = "URL_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";
  private static final Integer INT_BASE_VALUE = 1000;
  private static final Integer NUM_ROWS = 1024;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(URL_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
  private static final List<FieldConfig> FIELD_CONFIGS =
      List.of(new FieldConfig(DOMAIN_NAMES_COL, EncodingType.DICTIONARY, List.of(IndexType.FST), null, null),
          new FieldConfig(URL_COL, EncodingType.DICTIONARY, List.of(IndexType.FST), null, null));

  private TableConfig _tableConfig;
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

    List<IndexSegment> segments = new ArrayList<>();
    for (FSTType fstType : Arrays.asList(FSTType.LUCENE, FSTType.NATIVE)) {
      buildSegment(fstType);
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, SCHEMA);
      ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig,
          null);
      segments.add(segment);
    }

    _indexSegment = segments.get(ThreadLocalRandom.current().nextInt(2));
    _indexSegments = segments;
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<String> getURLSuffixes() {
    return Arrays.asList("/a", "/b", "/c", "/d");
  }

  private List<String> getNoIndexData() {
    return Arrays.asList("test1", "test2", "test3", "test4", "test5");
  }

  private List<String> getDomainNames() {
    return Arrays.asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd",
        "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
        "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
        "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    List<String> urlSuffixes = getURLSuffixes();
    List<String> noIndexData = getNoIndexData();
    for (int i = 0; i < NUM_ROWS; i++) {
      String domain = domainNames.get(i % domainNames.size());
      String url = domain + urlSuffixes.get(i % urlSuffixes.size());

      GenericRow row = new GenericRow();
      row.putValue(INT_COL_NAME, INT_BASE_VALUE + i);
      row.putValue(NO_INDEX_STRING_COL_NAME, noIndexData.get(i % noIndexData.size()));
      row.putValue(DOMAIN_NAMES_COL, domain);
      row.putValue(URL_COL, url);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(FSTType fstType)
      throws Exception {
    List<GenericRow> rows = createTestData();
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(FIELD_CONFIGS).build();
    _tableConfig.getIndexingConfig().setFSTIndexType(fstType);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void testInnerSegmentSelectionQuery(String query, int expectedResultSize,
      @Nullable List<Object[]> expectedResults) {
    Operator<SelectionResultsBlock> operator = getOperator(query);
    SelectionResultsBlock resultsBlock = operator.nextBlock();
    List<Object[]> results = (List<Object[]>) resultsBlock.getRows();
    assertNotNull(results);
    assertEquals(results.size(), expectedResultSize);
    if (expectedResults != null) {
      for (int i = 0; i < expectedResultSize; i++) {
        assertEquals(results.get(i), expectedResults.get(i));
      }
    }
  }

  private void testInterSegmentsSelectionQuery(String query, int expectedResultSize,
      @Nullable List<Object[]> expectedRows) {
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"INT_COL", "URL_COL"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    if (expectedRows != null) {
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, new ResultTable(expectedDataSchema, expectedRows));
    } else {
      ResultTable resultTable = brokerResponse.getResultTable();
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);
      assertEquals(resultTable.getRows().size(), expectedResultSize);
    }
  }

  private AggregationGroupByResult getGroupByResults(String query) {
    GroupByOperator groupByOrderByOperator = getOperator(query);
    return groupByOrderByOperator.nextBlock().getAggregationGroupByResult();
  }

  private void matchGroupResult(AggregationGroupByResult result, String key, long count) {
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = result.getGroupKeyIterator();
    boolean found = false;
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      if (groupKey._keys[0].equals(key)) {
        assertEquals(((Number) result.getResultForGroupId(0, groupKey._groupId)).longValue(), count);
        found = true;
      }
    }
    Assert.assertTrue(found);
  }

  private void testInterSegmentsCountQuery(String query, long expectedCount) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query),
        new ResultTable(new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}),
            Collections.singletonList(new Object[]{expectedCount})));
  }

  @Test
  public void testFSTBasedRegexLike() {
    // Select queries on col with FST + inverted index.
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*com') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    // Select queries on col with just FST index.
    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 5";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Object[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Object[]{1016, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.sd.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Object[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Object[]{1004, "www.sd.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1004, "www.sd.domain1.com/b"});
    expected.add(new Object[]{1008, "www.domain2.com/c"});
    expected.add(new Object[]{1012, "www.sd.domain2.co.cd/d"});
    expected.add(new Object[]{1016, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, null);
  }

  @Test
  public void testLikeOperator() {

    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.dom_in1.com' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 64, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.do_ai%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.sd.domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE '%domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE '%com' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);
  }

  @Test
  public void testFSTBasedRegexpLikeWithOtherFilters() {
    // Select queries on columns with combination of FST Index , (FST + Inverted Index), No index and other constraints.
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 52, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 51, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 13, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 0, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 12, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL=1000 LIMIT 50000";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 1, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test2') AND INT_COL=1001 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    testInnerSegmentSelectionQuery(query, 1, expected);
  }

  @Test
  public void testGroupByOnFSTBasedRegexpLike() {
    String query;
    query = "SELECT DOMAIN_NAMES, count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') GROUP BY "
        + "DOMAIN_NAMES LIMIT 50000";
    AggregationGroupByResult result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com", 64);
    matchGroupResult(result, "www.domain1.co.ab", 64);
    matchGroupResult(result, "www.domain1.co.bc", 64);
    matchGroupResult(result, "www.domain1.co.cd", 64);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 13);
    matchGroupResult(result, "www.sd.domain1.com/a", 13);
    matchGroupResult(result, "www.domain2.com/a", 13);
    matchGroupResult(result, "www.sd.domain2.com/a", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 13);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL > 1005 GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a') GROUP BY URL_COL "
        + "LIMIT 50000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 64);
  }

  @Test
  public void testInterSegment() {
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test2') AND INT_COL=1001 LIMIT 50000";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    testInterSegmentsSelectionQuery(query, 4, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL=1000 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    testInterSegmentsSelectionQuery(query, 4, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') ORDER  BY INT_COL LIMIT 5000";
    testInterSegmentsSelectionQuery(query, 48, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentsSelectionQuery(query, 0, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentsSelectionQuery(query, 52, null);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a')";
    testInterSegmentsCountQuery(query, 256);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') "
        + "AND INT_COL > 1005 ";
    testInterSegmentsCountQuery(query, 200);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentsCountQuery(query, 204);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentsCountQuery(query, 208);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*')";
    testInterSegmentsCountQuery(query, 1024);
  }
}
