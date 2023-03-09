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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
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
    indexLoadingConfig.setRangeIndexColumns(invertedIndexCols);
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
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(INT_COL_NAME, i);
      row.putValue(NO_INDEX_INT_COL_NAME, i);
      row.putValue(STATIC_INT_COL_NAME, 10);
      row.putValue(BOOLEAN_COL_NAME, RandomUtils.nextBoolean());
      row.putValue(STRING_COL_NAME, RandomStringUtils.randomAlphabetic(4));
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData();
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList(INT_COL_NAME)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(STATIC_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(BOOLEAN_COL_NAME, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
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
    String filterQuery = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable GROUP BY BOOLEAN_COL";
    String nonFilterQuery = "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 GROUP BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupByCaseAlternative() {
    String filterQuery =
        "SELECT SUM(INT_COL), SUM(INT_COL) FILTER(WHERE INT_COL > 25000) AS total_sum FROM MyTable GROUP BY "
            + "BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT SUM(INT_COL), SUM(CASE WHEN INT_COL > 25000 THEN INT_COL ELSE 0 END) AS total_sum FROM MyTable GROUP "
            + "BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testGroupBySameFilter() {
    String filterQuery =
        "SELECT AVG(INT_COL) FILTER(WHERE INT_COL > 25000) testAvg, SUM(INT_COL) FILTER(WHERE INT_COL > 25000) "
            + "testSum FROM MyTable GROUP BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT AVG(INT_COL) testAvg, SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 GROUP BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);
  }

  @Test
  public void testMultipleAggregationsOnSameFilterGroupBy() {
    String filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) testMin, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) testMax FROM MyTable GROUP BY BOOLEAN_COL";
    String nonFilterQuery =
        "SELECT MIN(INT_COL) testMin, MAX(INT_COL) testMax FROM MyTable WHERE INT_COL > 29990 GROUP BY BOOLEAN_COL";
    testQuery(filterQuery, nonFilterQuery);

    filterQuery = "SELECT MIN(INT_COL) FILTER(WHERE NO_INDEX_COL > 29990) AS total_min, "
        + "MAX(INT_COL) FILTER(WHERE INT_COL > 29990) AS total_max, "
        + "SUM(INT_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_sum, "
        + "MAX(NO_INDEX_COL) FILTER(WHERE NO_INDEX_COL < 5000) AS total_max2 FROM MyTable GROUP BY BOOLEAN_COL";
    nonFilterQuery = "SELECT MIN(CASE WHEN (NO_INDEX_COL > 29990) THEN INT_COL ELSE 99999 END) AS total_min, "
        + "MAX(CASE WHEN (INT_COL > 29990) THEN INT_COL ELSE 0 END) AS total_max, "
        + "SUM(CASE WHEN (NO_INDEX_COL < 5000) THEN INT_COL ELSE 0 END) AS total_sum, "
        + "MAX(CASE WHEN (NO_INDEX_COL < 5000) THEN NO_INDEX_COL ELSE 0 END) AS total_max2 FROM MyTable GROUP BY "
        + "BOOLEAN_COL";
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
    String filterQuery =
        "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 25000) testSum FROM MyTable";
    String nonFilterQuery =
        "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000";
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
    String nonFilterQuery =
        "SELECT SUM(INT_COL) testSum FROM MyTable WHERE INT_COL > 25000 AND INT_COL < 1000000";
    long filterQueryDocsScanned = getBrokerResponse(filterQuery).getNumDocsScanned();
    long nonFilterQueryDocsScanned = getBrokerResponse(nonFilterQuery).getNumDocsScanned();
    assertEquals(filterQueryDocsScanned, nonFilterQueryDocsScanned);
  }
}
