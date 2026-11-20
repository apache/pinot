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
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.response.broker.ResultTable;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FilteredAggregationsTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_COL";
  private static final String STATIC_INT_COL_NAME = "STATIC_INT_COL";
  private static final String BOOLEAN_COL_NAME = "BOOLEAN_COL";
  private static final String STRING_COL_NAME = "STRING_COL";
  private static final Integer NUM_ROWS = 30000;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(STATIC_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(BOOLEAN_COL_NAME, FieldSpec.DataType.BOOLEAN)
      .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
  private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COL_NAME)).setRangeIndexColumns(List.of(INT_COL_NAME))
      .setFieldConfigList(FIELD_CONFIGS).build();

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

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
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

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    Random random = new Random();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(INT_COL_NAME, i);
      row.putValue(NO_INDEX_INT_COL_NAME, i);
      row.putValue(STATIC_INT_COL_NAME, 10);
      row.putValue(BOOLEAN_COL_NAME, random.nextBoolean());
      row.putValue(STRING_COL_NAME, RandomStringUtils.secure().nextAlphabetic(4));
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void testQuery(String filterQuery, String nonFilterQuery) {
    ResultTable filterQueryResultTable = getBrokerResponse(filterQuery).getResultTable();
    ResultTable nonFilterQueryResultTable = getBrokerResponse(nonFilterQuery).getResultTable();
    assertEquals(filterQueryResultTable.getDataSchema(), nonFilterQueryResultTable.getDataSchema());
    List<Object[]> filterQueryRows = filterQueryResultTable.getRows();
    List<Object[]> nonFilterQueryRows = nonFilterQueryResultTable.getRows();
    assertEquals(filterQueryRows.size(), nonFilterQueryRows.size());
    for (int i = 0; i < filterQueryRows.size(); i++) {
      assertEquals(filterQueryRows.get(i), nonFilterQueryRows.get(i));
    }
  }

  @Test
  public void testSimpleQueries() {
    String filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 9999) sum1 FROM MyTable WHERE INT_COL < 1000000";
    String nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE INT_COL > 9999 AND INT_COL < 1000000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL < 3) sum1 FROM MyTable WHERE INT_COL > 1";
    nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE INT_COL > 1 AND INT_COL < 3";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT COUNT(*) FILTER(WHERE INT_COL = 4) count1 FROM MyTable";
    nonFilterQuery = "SELECT COUNT(*) count1 FROM MyTable WHERE INT_COL = 4";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 8000) sum1 FROM MyTable ";
    nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE INT_COL > 8000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE NO_INDEX_COL <= 1) sum1 FROM MyTable WHERE INT_COL > 1";
    nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE NO_INDEX_COL <= 1 AND INT_COL > 1";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT AVG(NO_INDEX_COL) avg1 FROM MyTable WHERE NO_INDEX_COL > -1";
    nonFilterQuery = "SELECT AVG(NO_INDEX_COL) avg1 FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT AVG(INT_COL) FILTER(WHERE NO_INDEX_COL > -1) avg1 FROM MyTable";
    nonFilterQuery = "SELECT AVG(INT_COL) avg1 FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery =
        "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) min1, MAX(INT_COL) FILTER(WHERE INT_COL > 29990) max1"
            + " FROM MyTable";
    nonFilterQuery = "SELECT MIN(INT_COL) min1, MAX(INT_COL) max1 FROM MyTable WHERE INT_COL > 29990";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE BOOLEAN_COL) sum1 FROM MyTable";
    nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE BOOLEAN_COL=true";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE BOOLEAN_COL AND STARTSWITH(STRING_COL, 'abc')) sum1 FROM MyTable";
    nonFilterQuery = "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE BOOLEAN_COL=true AND STARTSWITH(STRING_COL, 'abc')";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE BOOLEAN_COL AND STARTSWITH(REVERSE(STRING_COL), 'abc')) sum1 FROM MyTable";
    nonFilterQuery =
        "SELECT SUM(INT_COL) sum1 FROM MyTable WHERE BOOLEAN_COL=true AND STARTSWITH(REVERSE(STRING_COL), " + "'abc')";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testFilterResultColumnNameGroupBy() {
    String filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 9999) FROM MyTable WHERE INT_COL < 1000000 GROUP BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT SUM(INT_COL) \"sum(INT_COL) FILTER(WHERE INT_COL > '9999')\" FROM MyTable WHERE INT_COL > 9999 AND "
            + "INT_COL < 1000000 GROUP BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 9999 AND INT_COL < 1000000) FROM MyTable GROUP BY BOOLEAN_COL";
    nonFilterQuery =
        "SELECT SUM(INT_COL) \"sum(INT_COL) FILTER(WHERE (INT_COL > '9999' AND INT_COL < '1000000'))\" FROM MyTable "
            + "WHERE INT_COL > 9999 AND INT_COL < 1000000 GROUP BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testFilterResultColumnNameNonGroupBy() {
    String filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 9999) FROM MyTable WHERE INT_COL < 1000000";
    String nonFilterQuery =
        "SELECT SUM(INT_COL) \"sum(INT_COL) FILTER(WHERE INT_COL > '9999')\" FROM MyTable WHERE INT_COL > 9999 AND "
            + "INT_COL < 1000000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 9999 AND INT_COL < 1000000) FROM MyTable";
    nonFilterQuery =
        "SELECT SUM(INT_COL) \"sum(INT_COL) FILTER(WHERE (INT_COL > '9999' AND INT_COL < '1000000'))\" FROM MyTable "
            + "WHERE INT_COL > 9999 AND INT_COL < 1000000";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testFilterVsCase() {
    String filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 1234 AND INT_COL < 22000) AS total_sum FROM MyTable";
    String nonFilterQuery =
        "SELECT SUM(CASE WHEN (INT_COL > 1234 AND INT_COL < 22000) THEN INT_COL ELSE 0 END) AS total_sum FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL % 10 = 0) AS total_sum, SUM(NO_INDEX_COL), MAX(INT_COL) FROM MyTable";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL % 10 = 0) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(NO_INDEX_COL), MAX(INT_COL) FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL % 10 = 0) AS total_sum, MAX(NO_INDEX_COL) FROM MyTable";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL % 10 = 0) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(NO_INDEX_COL) FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL % 10 = 0) AS total_sum, "
        + "MAX(NO_INDEX_COL) FROM MyTable WHERE NO_INDEX_COL > 5";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL % 10 = 0) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(NO_INDEX_COL) FROM MyTable WHERE NO_INDEX_COL > 5";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MAX(INT_COL) FILTER(WHERE INT_COL < 100) AS total_max FROM MyTable";
    nonFilterQuery = "SELECT MAX(CASE WHEN (INT_COL < 100) THEN INT_COL ELSE 0 END) AS total_max FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MIN(NO_INDEX_COL) FILTER(WHERE INT_COL < 100) AS total_min FROM MyTable";
    nonFilterQuery = "SELECT MIN(CASE WHEN (INT_COL < 100) THEN NO_INDEX_COL ELSE 0 END) AS total_min FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 3) AS total_sum, "
        + "SUM(INT_COL) FILTER(WHERE INT_COL < 4) AS total_sum2 FROM MyTable WHERE INT_COL > 2";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL > 3) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN (INT_COL < 4) THEN INT_COL ELSE 0 END) AS total_sum2 FROM MyTable WHERE INT_COL > 2";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 12345) AS total_sum, "
        + "SUM(INT_COL) FILTER(WHERE INT_COL < 59999) AS total_sum2, "
        + "MIN(INT_COL) FILTER(WHERE INT_COL > 5000) AS total_min FROM MyTable WHERE INT_COL > 1000";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL > 12345) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN (INT_COL < 59999) THEN INT_COL ELSE 0 END) AS total_sum2, "
        + "MIN(CASE WHEN (INT_COL > 5000) THEN INT_COL ELSE 9999999 END) AS total_min "
        + "FROM MyTable WHERE INT_COL > 1000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE NO_INDEX_COL > 12345) AS total_sum, "
        + "SUM(INT_COL) FILTER(WHERE NO_INDEX_COL < 59999) AS total_sum2, "
        + "MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 5000) AS total_min FROM MyTable WHERE INT_COL > 1000";
    nonFilterQuery = "SELECT SUM(CASE WHEN (NO_INDEX_COL > 12345) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN (NO_INDEX_COL < 59999) THEN INT_COL ELSE 0 END) AS total_sum2, "
        + "MIN(CASE WHEN (NO_INDEX_COL > 5000) THEN INT_COL ELSE 9999999 END) AS total_min "
        + "FROM MyTable WHERE INT_COL > 1000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 12345) AS total_sum, "
        + "SUM(NO_INDEX_COL) FILTER(WHERE INT_COL < 59999) AS total_sum2, "
        + "MIN(INT_COL) FILTER(WHERE INT_COL > 5000) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL > 12345) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN (INT_COL < 59999) THEN NO_INDEX_COL ELSE 0 END) AS total_sum2, "
        + "MIN(CASE WHEN (INT_COL > 5000) THEN INT_COL ELSE 9999999 END) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE ABS(INT_COL) > 12345) AS total_sum, "
        + "SUM(NO_INDEX_COL) FILTER(WHERE LN(INT_COL) < 59999) AS total_sum2, "
        + "MIN(INT_COL) FILTER(WHERE INT_COL > 5000) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    nonFilterQuery = "SELECT SUM(CASE WHEN (ABS(INT_COL) > 12345) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN (LN(INT_COL) < 59999) THEN NO_INDEX_COL ELSE 0 END) AS total_sum2, "
        + "MIN(CASE WHEN (INT_COL > 5000) THEN INT_COL ELSE 9999999 END) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE MOD(INT_COL, STATIC_INT_COL) = 0) AS total_sum, "
        + "MIN(INT_COL) FILTER(WHERE INT_COL > 5000) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    nonFilterQuery = "SELECT SUM(CASE WHEN (MOD(INT_COL, STATIC_INT_COL) = 0) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MIN(CASE WHEN (INT_COL > 5000) THEN INT_COL ELSE 9999999 END) AS total_min "
        + "FROM MyTable WHERE INT_COL < 28000 AND NO_INDEX_COL > 3000";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 123 AND INT_COL < 25000) AS total_sum, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 123 AND INT_COL < 25000) AS total_max "
        + "FROM MyTable WHERE NO_INDEX_COL > 5 AND NO_INDEX_COL < 29999";
    nonFilterQuery = "SELECT SUM(CASE WHEN (INT_COL > 123 AND INT_COL < 25000) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(CASE WHEN (INT_COL > 123 AND INT_COL < 25000) THEN INT_COL ELSE 0 END) AS total_max "
        + "FROM MyTable WHERE NO_INDEX_COL > 5 AND NO_INDEX_COL < 29999";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testMultipleAggregationsOnSameFilter() {
    String filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) testMin, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) testMax FROM MyTable";
    String nonFilterQuery = "SELECT MIN(INT_COL) testMin, MAX(INT_COL) testMax FROM MyTable WHERE INT_COL > 29990";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) AS total_min, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) AS total_max, "
        + "SUM(INT_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_sum, "
        + "MAX(NO_INDEX_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_max2 FROM MyTable";
    nonFilterQuery = "SELECT MIN(CASE WHEN (NO_INDEX_COL > 29990) THEN INT_COL ELSE 99999 END) AS total_min, "
        + "MAX(CASE WHEN (INT_COL > 29990) THEN INT_COL ELSE 0 END) AS total_max, "
        + "SUM(CASE WHEN (NO_INDEX_COL < 5000) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(CASE WHEN (NO_INDEX_COL < 5000) THEN NO_INDEX_COL ELSE 0 END) AS total_max2 FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testMultipleAggregationsOnSameFilterOrderByFiltered() {
    String filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) testMin, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) testMax FROM MyTable ORDER BY testMax";
    String nonFilterQuery =
        "SELECT MIN(INT_COL) testMin, MAX(INT_COL) testMax FROM MyTable WHERE INT_COL > 29990 ORDER BY testMax";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) AS total_min, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) AS total_max, "
        + "SUM(INT_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_sum, "
        + "MAX(NO_INDEX_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_max2 FROM MyTable ORDER BY total_sum";
    nonFilterQuery = "SELECT MIN(CASE WHEN (NO_INDEX_COL > 29990) THEN INT_COL ELSE 99999 END) AS total_min, "
        + "MAX(CASE WHEN (INT_COL > 29990) THEN INT_COL ELSE 0 END) AS total_max, "
        + "SUM(CASE WHEN (NO_INDEX_COL < 5000) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(CASE WHEN (NO_INDEX_COL < 5000) THEN NO_INDEX_COL ELSE 0 END) AS total_max2 FROM MyTable ORDER BY "
        + "total_sum";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testMixedAggregationsOfSameType() {
    String filterQuery = "SELECT SUM(INT_COL), SUM(INT_COL) FILTER(WHERE INT_COL > 25000) AS total_sum FROM MyTable";
    String nonFilterQuery =
        "SELECT SUM(INT_COL), SUM(CASE WHEN INT_COL > 25000 THEN INT_COL ELSE 0 END) AS total_sum FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT SUM(INT_COL), SUM(INT_COL) FILTER(WHERE INT_COL < 5000) AS total_sum, "
        + "SUM(INT_COL) FILTER(WHERE INT_COL > 12345) AS total_sum2 FROM MyTable";
    nonFilterQuery = "SELECT SUM(INT_COL), SUM(CASE WHEN INT_COL < 5000 THEN INT_COL ELSE 0 END) AS total_sum, "
        + "SUM(CASE WHEN INT_COL > 12345 THEN INT_COL ELSE 0 END) AS total_sum2 FROM MyTable";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupBy() {
    String filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable GROUP BY BOOLEAN_COL "
        + "ORDER BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupByMultipleColumns() {
    String filterQuery = "SET " + CommonConstants.Broker.Request.QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS
        + "=true; SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable GROUP BY BOOLEAN_COL, "
        + "STRING_COL ORDER BY BOOLEAN_COL, STRING_COL";
    String nonFilterQuery =
        "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 GROUP BY BOOLEAN_COL, STRING_COL "
            + "ORDER BY BOOLEAN_COL, STRING_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupByCaseAlternative() {
    String filterQuery = "SELECT SUM(INT_COL), SUM(INT_COL) FILTER(WHERE INT_COL > 25000) AS total_sum FROM MyTable "
        + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT SUM(INT_COL), SUM(CASE WHEN INT_COL > 25000 THEN INT_COL ELSE 0 END) AS total_sum FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupBySameFilter() {
    String filterQuery =
        "SELECT AVG(INT_COL) FILTER(WHERE INT_COL > 25000) testAvg, SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum "
            + "FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    String nonFilterQuery = "SELECT AVG(INT_COL) testAvg, SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 "
        + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testMultipleAggregationsOnSameFilterGroupBy() {
    String filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) testMin, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) testMax FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT MIN(INT_COL) testMin, MAX(INT_COL) testMax FROM MyTable WHERE INT_COL > 29990 GROUP BY BOOLEAN_COL "
            + "ORDER BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) AS total_min, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) AS total_max, "
        + "SUM(INT_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_sum, "
        + "MAX(NO_INDEX_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_max2 "
        + "FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    nonFilterQuery = "SELECT MIN(CASE WHEN (NO_INDEX_COL > 29990) THEN INT_COL ELSE 99999 END) AS total_min, "
        + "MAX(CASE WHEN (INT_COL > 29990) THEN INT_COL ELSE 0 END) AS total_max, "
        + "SUM(CASE WHEN (NO_INDEX_COL < 5000) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(CASE WHEN (NO_INDEX_COL < 5000) THEN NO_INDEX_COL ELSE 0 END) AS total_max2 "
        + "FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupBySameFilterOrderByFiltered() {
    String filterQuery =
        "SELECT AVG(INT_COL) FILTER(WHERE INT_COL > 25000) testAvg, SUM(INT_COL) FILTER(WHERE INT_COL > 25000) "
            + "testSum FROM MyTable GROUP BY BOOLEAN_COL ORDER BY testAvg";
    String nonFilterQuery =
        "SELECT AVG(INT_COL) testAvg, SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 GROUP BY BOOLEAN_COL "
            + "ORDER BY testAvg";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testSameNumScannedFilteredAggMatchAll() {
    // For a single filtered aggregation, the same number of docs should be scanned regardless of which portions of
    // the filter are in the filter expression Vs. the main predicate i.e. the applied filters are commutative.
    String filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable";
    String nonFilterQuery = "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000";
    long filterQueryDocsScanned = getBrokerResponse(filterQuery).getNumDocsScanned();
    long nonFilterQueryDocsScanned = getBrokerResponse(nonFilterQuery).getNumDocsScanned();
    assertEquals(filterQueryDocsScanned, nonFilterQueryDocsScanned);
  }

  @Test
  public void testSameNumScannedFilteredAgg() {
    // For a single filtered aggregation, the same number of docs should be scanned regardless of which portions of
    // the filter are in the filter expression Vs. the main predicate i.e. the applied filters are commutative.
    String filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable WHERE INT_COL < 1000000";
    String nonFilterQuery = "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 AND INT_COL < 1000000";
    long filterQueryDocsScanned = getBrokerResponse(filterQuery).getNumDocsScanned();
    long nonFilterQueryDocsScanned = getBrokerResponse(nonFilterQuery).getNumDocsScanned();
    assertEquals(filterQueryDocsScanned, nonFilterQueryDocsScanned);
  }

  @Test
  public void testFilteredAggregationOnlyInHaving() {
    // An aggregation with a FILTER clause that is referenced only in the HAVING clause (and not in the SELECT list)
    // must still have its filter applied. With the main filter INT_COL < 8, each group holds a single distinct
    // INT_COL value, so groups 0-4 hold only rows matching INT_COL < 5 and groups 5-7 only rows matching
    // INT_COL >= 5. The HAVING clause therefore keeps groups 0-4.
    String havingQuery =
        "SELECT NO_INDEX_COL, COUNT(*) testCount FROM MyTable WHERE INT_COL < 8 GROUP BY NO_INDEX_COL "
            + "HAVING COUNT(*) FILTER(WHERE INT_COL < 5) > 0 AND COUNT(*) FILTER(WHERE INT_COL >= 5) < 1 "
            + "ORDER BY NO_INDEX_COL";
    List<Object[]> rows = getBrokerResponse(havingQuery).getResultTable().getRows();
    assertEquals(rows.size(), 5);
    for (int i = 0; i < 5; i++) {
      assertEquals(rows.get(i)[0], i);
      assertEquals(rows.get(i)[1], 4L);
    }

    // Projecting the same filtered aggregations in the SELECT list must not change the groups that are kept.
    String selectQuery =
        "SELECT NO_INDEX_COL, COUNT(*) testCount, COUNT(*) FILTER(WHERE INT_COL < 5) testLow, "
            + "COUNT(*) FILTER(WHERE INT_COL >= 5) testHigh FROM MyTable WHERE INT_COL < 8 GROUP BY NO_INDEX_COL "
            + "HAVING COUNT(*) FILTER(WHERE INT_COL < 5) > 0 AND COUNT(*) FILTER(WHERE INT_COL >= 5) < 1 "
            + "ORDER BY NO_INDEX_COL";
    List<Object[]> selectRows = getBrokerResponse(selectQuery).getResultTable().getRows();
    assertEquals(selectRows.size(), rows.size());
    for (int i = 0; i < selectRows.size(); i++) {
      assertEquals(selectRows.get(i)[0], rows.get(i)[0]);
      assertEquals(selectRows.get(i)[1], rows.get(i)[1]);
      assertEquals(selectRows.get(i)[2], 4L);
      assertEquals(selectRows.get(i)[3], 0L);
    }
  }

  @Test
  public void testFilteredAggregationOnlyInOrderBy() {
    // Same as above for an aggregation that is referenced only in the ORDER-BY clause. Groups 5-7 hold only rows
    // matching INT_COL >= 5, so they must sort before groups 0-4 on the filtered count.
    String query = "SELECT NO_INDEX_COL FROM MyTable WHERE INT_COL < 8 GROUP BY NO_INDEX_COL "
        + "ORDER BY COUNT(*) FILTER(WHERE INT_COL >= 5) DESC, NO_INDEX_COL ASC";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    assertEquals(rows.size(), 8);
    int[] groups = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      groups[i] = (int) rows.get(i)[0];
    }
    assertEquals(groups, new int[]{5, 6, 7, 0, 1, 2, 3, 4});
  }

  @Test
  public void testFilteredAggregationOnlyInOrderBySkipEmptyGroups() {
    // The query below has no non-filtered aggregation, so enabling the skip-empty-groups option drops the groups that
    // match no aggregation filter. Groups 0-4 hold no row matching INT_COL >= 5 and are therefore not returned.
    String query = "SET " + CommonConstants.Broker.Request.QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS
        + "=true; SELECT NO_INDEX_COL FROM MyTable WHERE INT_COL < 8 GROUP BY NO_INDEX_COL "
        + "ORDER BY COUNT(*) FILTER(WHERE INT_COL >= 5) DESC, NO_INDEX_COL ASC";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    int[] groups = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      groups[i] = (int) rows.get(i)[0];
    }
    assertEquals(groups, new int[]{5, 6, 7});
  }

  @Test
  public void testFilteredAggregationOnlyInHavingWithoutGroupBy() {
    // Without a GROUP BY the query is planned by AggregationPlanNode, which switches to the filtered operator here
    // too. The main filter matches INT_COL 0-7 across the 4 segments, and the HAVING condition holds either way, so
    // the projected count must stay the same.
    String query = "SELECT COUNT(*) testCount FROM MyTable WHERE INT_COL < 8 "
        + "HAVING COUNT(*) FILTER(WHERE INT_COL < 5) > 0";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 32L);
  }
}
