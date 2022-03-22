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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


/**
 * Queries test for DISTINCT_COUNT_THETA_SKETCH queries.
 */
@SuppressWarnings("unchecked")
public class UrlFunctionGroupByTest extends BaseQueriesTest {
  private static final String[] PLAINTEXT_STRINGS = new String[]{
      "I/O>kH@cN]4I\\*n' k /kW",
      "fTN]V{!$ }2%`Ts6P\\1A#",
      "'qk &K[,U3(w1I[`A|~ge",
      "\\ 1l !@7Qs9to6b{\\L $PnE",
      "@v*Ga/p4$EK'- C?l77c+",
      "&buE(EL+33G` kfk t6qdJ",
      "bK\" U9_6n _;{#?l22 H:Kl",
      "p} tpJ[M~{Mz ] MIL<lDKx",
      "Yd}\"C7e v)1TN DwRPLH*^",
      "8iRug 30Yuh;z| r{):p{9",
      "* nI(RQ&@Y^%fYgM ?/ 8IB",
      "7`N)F{HM[63.:!g q`Q,u",
      "[=,  x (,79{,3]M<6YB>:2",
      "U+W4c# {F*`k_6nDvr\\ JP",
      "@ H5*6i&_} RE#T 38/80b{",
      "[p~5=xT)Jb|VSlu\"t 'c",
      "O|.}7ro { {8_S`@amd^Lc",
      "Y O -TZ[M Z#56$4`qb9TAM",
      "qLNZ\"  Nuf L?H>*bMYs?yz",
      "v=)}X01(1u[G4 +M J'qmA"
  };

  private static final String[] PLAINTEXT_QUERY_STRINGS = new String[]{
      "I/O>kH@cN]4I\\*n'' k /kW",
      "fTN]V{!$ }2%`Ts6P\\1A#",
      "''qk &K[,U3(w1I[`A|~ge",
      "\\ 1l !@7Qs9to6b{\\L $PnE",
      "@v*Ga/p4$EK''- C?l77c+",
      "&buE(EL+33G` kfk t6qdJ",
      "bK\" U9_6n _;{#?l22 H:Kl",
      "p} tpJ[M~{Mz ] MIL<lDKx",
      "Yd}\"C7e v)1TN DwRPLH*^",
      "8iRug 30Yuh;z| r{):p{9",
      "* nI(RQ&@Y^%fYgM ?/ 8IB",
      "7`N)F{HM[63.:!g q`Q,u",
      "[=,  x (,79{,3]M<6YB>:2",
      "U+W4c# {F*`k_6nDvr\\ JP",
      "@ H5*6i&_} RE#T 38/80b{",
      "[p~5=xT)Jb|VSlu\"t ''c",
      "O|.}7ro { {8_S`@amd^Lc",
      "Y O -TZ[M Z#56$4`qb9TAM",
      "qLNZ\"  Nuf L?H>*bMYs?yz",
      "v=)}X01(1u[G4 +M J''qmA"
  };

  private static final String[] ENCODED_STRINGS = new String[]{
      "I%2FO%3EkH%40cN%5D4I%5C*n%27+k+%2FkW",
      "fTN%5DV%7B%21%24+%7D2%25%60Ts6P%5C1A%23",
      "%27qk+%26K%5B%2CU3%28w1I%5B%60A%7C%7Ege",
      "%5C+1l+%21%407Qs9to6b%7B%5CL+%24PnE",
      "%40v*Ga%2Fp4%24EK%27-+C%3Fl77c%2B",
      "%26buE%28EL%2B33G%60+kfk+t6qdJ",
      "bK%22+U9_6n+_%3B%7B%23%3Fl22+H%3AKl",
      "p%7D+tpJ%5BM%7E%7BMz+%5D+MIL%3ClDKx",
      "Yd%7D%22C7e+v%291TN+DwRPLH*%5E",
      "8iRug+30Yuh%3Bz%7C+r%7B%29%3Ap%7B9",
      "*+nI%28RQ%26%40Y%5E%25fYgM+%3F%2F+8IB",
      "7%60N%29F%7BHM%5B63.%3A%21g+q%60Q%2Cu",
      "%5B%3D%2C++x+%28%2C79%7B%2C3%5DM%3C6YB%3E%3A2",
      "U%2BW4c%23+%7BF*%60k_6nDvr%5C+JP",
      "%40+H5*6i%26_%7D+RE%23T+38%2F80b%7B",
      "%5Bp%7E5%3DxT%29Jb%7CVSlu%22t+%27c",
      "O%7C.%7D7ro+%7B+%7B8_S%60%40amd%5ELc",
      "Y+O+-TZ%5BM+Z%2356%244%60qb9TAM",
      "qLNZ%22++Nuf+L%3FH%3E*bMYs%3Fyz",
      "v%3D%29%7DX01%281u%5BG4+%2BM+J%27qmA"
  };

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "UrlFunctionGroupByTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 210;

  private static final String STRING_SV_PLAINTEXT = "stringSVPlaintext";
  private static final String STRING_SV_ENCODED = "stringSVEncoded";
  private static final String INT_SV_COLUMN = "intSVColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_SV_COLUMN, DataType.INT)
      .addSingleValueDimension(STRING_SV_PLAINTEXT, DataType.STRING)
      .addSingleValueDimension(STRING_SV_ENCODED, DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  public static void testAggregationGroupByResultHelper(AggregationGroupByResult aggregationGroupByResult,
      String[] resultsSet) {
    Iterator<GroupKeyGenerator.StringGroupKey> groupKeyIterator = aggregationGroupByResult.getStringGroupKeyIterator();
    int total = 0;
    while (groupKeyIterator.hasNext()) {
      total++;
      GroupKeyGenerator.StringGroupKey groupKey = groupKeyIterator.next();
      long resultForKey = (Long) aggregationGroupByResult.getResultForKey(groupKey, 0);
      Assert.assertEquals(groupKey._stringKey, resultsSet[(int) resultForKey - 1]);
    }
    Assert.assertEquals(total, ENCODED_STRINGS.length);
  }

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
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);

    int j = 1;
    int sum = 1;
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_SV_COLUMN, i);
      if (i >= sum) {
        sum += ++j;
      }
      record.putValue(STRING_SV_PLAINTEXT, PLAINTEXT_STRINGS[j - 1]);
      record.putValue(STRING_SV_ENCODED, ENCODED_STRINGS[j - 1]);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testInterSegment() {
    for (int i = 0; i < 20; i++) {
      String query = "SELECT COUNT(*) FROM " + RAW_TABLE_NAME
          + " WHERE " + STRING_SV_ENCODED + "= encodeUrl('" + PLAINTEXT_QUERY_STRINGS[i] + "')";
      testInterSegmentAggregationQueryHelper(query, i + 1);
    }
    for (int i = 0; i < 20; i++) {
      String query = "SELECT COUNT(*) FROM " + RAW_TABLE_NAME
          + " WHERE " + STRING_SV_PLAINTEXT + "= decodeUrl('" + ENCODED_STRINGS[i] + "')";
      testInterSegmentAggregationQueryHelper(query, i + 1);
    }
  }

  @Test
  public void testGroupBy() {
    String query = "SELECT " + STRING_SV_ENCODED + " , COUNT(*) FROM " + RAW_TABLE_NAME
        + " GROUP BY " + STRING_SV_ENCODED;
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForSqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    testAggregationGroupByResultHelper(aggregationGroupByResult, ENCODED_STRINGS);

    query = "SELECT " + STRING_SV_PLAINTEXT + " , COUNT(*) FROM " + RAW_TABLE_NAME
        + " GROUP BY " + STRING_SV_PLAINTEXT;
    aggregationGroupByOperator = getOperatorForSqlQuery(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    testAggregationGroupByResultHelper(aggregationGroupByResult, PLAINTEXT_STRINGS);
  }

  private void testInterSegmentAggregationQueryHelper(String query, long expectedCount) {
    BrokerResponseNative brokerResponseNative = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.size(), 1);
    Assert.assertEquals(dataSchema.getColumnName(0), "count(*)");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    Assert.assertEquals(row.length, 1);
    Assert.assertEquals(row[0], expectedCount * 4);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
