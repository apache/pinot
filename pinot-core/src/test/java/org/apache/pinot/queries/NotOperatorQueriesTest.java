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
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
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


public class NotOperatorQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NotOperatorQueriesTest");
  private static final String TABLE_NAME = "MyTable";
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

    List<IndexSegment> segments = new ArrayList<>();
    buildSegment();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(FIRST_INT_COL_NAME);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    Set<String> fstIndexCols = new HashSet<>();
    fstIndexCols.add(DOMAIN_NAMES_COL);
    indexLoadingConfig.setFSTIndexColumns(fstIndexCols);
    indexLoadingConfig.setFSTIndexType(FSTType.LUCENE);
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    segments.add(segment);

    _indexSegment = segment;
    _indexSegments = segments;
  }

  private List<String> getDomainNames() {
    return Arrays.asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd",
        "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
        "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
        "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putField(FIRST_INT_COL_NAME, i);
      row.putField(SECOND_INT_COL_NAME, INT_BASE_VALUE + i);
      row.putField(DOMAIN_NAMES_COL, domain);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null));
    fieldConfigs.add(
        new FieldConfig(FIRST_INT_COL_NAME, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.INVERTED, null,
            null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(FIRST_INT_COL_NAME)).setFieldConfigList(fieldConfigs).build();
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

  private void testSelectionResults(String query, int expectedResultSize, List<Serializable[]> expectedResults) {
    Operator<IntermediateResultsBlock> operator = getOperatorForSqlQuery(query);
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

  @Test
  public void testLikeBasedNotOperator() {
    String query =
        "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') LIMIT "
            + "50000";
    testSelectionResults(query, 768, null);

    query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*') "
        + "LIMIT 50000";
    testSelectionResults(query, 768, null);

    query =
        "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*') LIMIT "
            + "50000";
    testSelectionResults(query, 512, null);

    query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*') LIMIT "
        + "50000";
    testSelectionResults(query, 0, null);

    query =
        "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT REGEXP_LIKE(DOMAIN_NAMES, '.*com') LIMIT 50000";
    testSelectionResults(query, 768, null);
  }

  @Test
  public void testWeirdPredicates() {
    String query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT FIRST_INT_COL = 5 LIMIT " + "50000";
    testSelectionResults(query, 1023, null);

    query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT FIRST_INT_COL < 5 LIMIT " + "50000";
    testSelectionResults(query, 1019, null);

    query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT FIRST_INT_COL > 5 LIMIT " + "50000";
    testSelectionResults(query, 6, null);
  }

  @Test
  public void testCompositePredicates() {
    String query =
        "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT (FIRST_INT_COL > 5 AND SECOND_INT_COL < 1009) "
            + "LIMIT " + "50000";
    testSelectionResults(query, 1021, null);

    query = "SELECT FIRST_INT_COL, SECOND_INT_COL FROM MyTable WHERE NOT (FIRST_INT_COL < 5 OR SECOND_INT_COL > 2000) "
        + "LIMIT " + "50000";
    testSelectionResults(query, 996, null);
  }
}
