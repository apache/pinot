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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public abstract class BaseFSTBasedRegexpLikeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), BaseFSTBasedRegexpLikeQueriesTest.class.getSimpleName());
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static List<FieldConfig> getFieldConfigs(String indexType) {
    try {
      // Create index configuration JSON
      ObjectNode indexConfig = OBJECT_MAPPER.createObjectNode();
      indexConfig.put("type", "LUCENE");

      ObjectNode indexes = OBJECT_MAPPER.createObjectNode();
      indexes.set(indexType, indexConfig);

      // Create FieldConfig with the index configuration
      return List.of(
          new FieldConfig(DOMAIN_NAMES_COL, EncodingType.DICTIONARY, null, null, null, null, indexes, null, null),
          new FieldConfig(URL_COL, EncodingType.DICTIONARY, null, null, null, null, indexes, null, null));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create field configs", e);
    }
  }

  private TableConfig _tableConfig;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  /**
   * Abstract method to be implemented by derived classes to specify index type.
   */
  protected abstract String getIndexType();

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
      buildSegment(fstType, getIndexType());
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, SCHEMA);
      ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
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

  private void buildSegment(FSTType fstType, String indexType)
      throws Exception {
    List<GenericRow> rows = createTestData();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(getFieldConfigs(indexType)).build();
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

  protected void testInnerSegmentSelectionQuery(String query, int expectedResultSize,
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

  protected void testInterSegmentsSelectionQuery(String query, int expectedResultSize,
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

  protected AggregationGroupByResult getGroupByResults(String query) {
    GroupByOperator groupByOrderByOperator = getOperator(query);
    return groupByOrderByOperator.nextBlock().getAggregationGroupByResult();
  }

  protected void matchGroupResult(AggregationGroupByResult result, String key, long count) {
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

  protected void testInterSegmentsCountQuery(String query, long expectedCount) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query),
        new ResultTable(new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}),
            Collections.singletonList(new Object[]{expectedCount})));
  }
}
