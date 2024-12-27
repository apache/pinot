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
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class NotOperatorQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NotOperatorQueriesTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String FIRST_INT_COL_NAME = "FIRST_INT_COL";
  private static final String SECOND_INT_COL_NAME = "SECOND_INT_COL";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
  private static final Integer INT_BASE_VALUE = 1000;
  private static final Integer NUM_ROWS = 1024;

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
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
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
    for (int i = 0; i < NUM_ROWS; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putValue(FIRST_INT_COL_NAME, i);
      row.putValue(SECOND_INT_COL_NAME, INT_BASE_VALUE + i);
      row.putValue(DOMAIN_NAMES_COL, domain);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(FIRST_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
        .addMetric(SECOND_INT_COL_NAME, FieldSpec.DataType.INT).build();
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

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void testNotOperator(String filter, long expectedSegmentResult) {
    String query = "SELECT COUNT(*) FROM testTable WHERE " + filter;
    BaseOperator<AggregationResultsBlock> aggregationOperator = getOperator(query);
    List<Object> segmentResults = aggregationOperator.nextBlock().getResults();
    assertNotNull(segmentResults);
    assertEquals((long) segmentResults.get(0), expectedSegmentResult);

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTableRows resultTableRows = brokerResponse.getResultTable();
    Object[] brokerResults = resultTableRows.getRows().get(0);
    assertEquals((long) brokerResults[0], 4 * expectedSegmentResult);
  }

  @Test
  public void testLikePredicates() {
    testNotOperator("DOMAIN_NAMES NOT LIKE 'www.domain1%'", 768);
    testNotOperator("NOT REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*')", 768);

    testNotOperator("DOMAIN_NAMES NOT LIKE 'www.sd.domain1%'", 768);
    testNotOperator("NOT REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*')", 768);

    testNotOperator("DOMAIN_NAMES NOT LIKE '%domain1%'", 512);
    testNotOperator("NOT REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*')", 512);

    testNotOperator("DOMAIN_NAMES NOT LIKE '%domain%'", 0);
    testNotOperator("NOT REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*')", 0);

    testNotOperator("DOMAIN_NAMES NOT LIKE '%com'", 768);
    testNotOperator("NOT REGEXP_LIKE(DOMAIN_NAMES, '.*com')", 768);
  }

  @Test
  public void testRangePredicates() {
    testNotOperator("NOT FIRST_INT_COL = 5", 1023);
    testNotOperator("NOT FIRST_INT_COL < 5", 1019);
    testNotOperator("NOT FIRST_INT_COL > 5", 6);

    testNotOperator("FIRST_INT_COL NOT BETWEEN 10 AND 20", 1013);
    testNotOperator("NOT FIRST_INT_COL BETWEEN 10 AND 20", 1013);
  }

  @Test
  public void testCompositePredicates() {
    testNotOperator("NOT (FIRST_INT_COL > 5 AND SECOND_INT_COL < 1009)", 1021);
    testNotOperator("NOT FIRST_INT_COL > 5 OR NOT SECOND_INT_COL < 1009", 1021);

    testNotOperator("NOT (FIRST_INT_COL < 5 OR SECOND_INT_COL > 2000)", 996);
    testNotOperator("NOT FIRST_INT_COL < 5 AND NOT SECOND_INT_COL > 2000", 996);
  }
}
