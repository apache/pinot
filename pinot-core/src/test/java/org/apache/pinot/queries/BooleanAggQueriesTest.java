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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GapfillUtils;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class BooleanAggQueriesTest extends BaseQueriesTest {

  private static final int NUM_RECORDS = 16;

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BooleanAggQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String BOOLEAN_COLUMN = "boolColumn";
  private static final String GROUP_BY_COLUMN = "groupByColumn";

  private static final Schema SCHEMA =
      new Schema.SchemaBuilder()
          .addSingleValueDimension(BOOLEAN_COLUMN, FieldSpec.DataType.BOOLEAN)
          .addSingleValueDimension(GROUP_BY_COLUMN, FieldSpec.DataType.STRING).build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @DataProvider
  public static Object[][] nullHandling() {
    return new Object[][] {
        new Object[]{true},
        new Object[]{false},
    };
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

    Object[][] recordContents = new Object[][] {
        new Object[]{true, "allTrue"},
        new Object[]{true, "allTrue"},
        new Object[]{true, "allTrue"},
        new Object[]{false, "allFalse"},
        new Object[]{false, "allFalse"},
        new Object[]{false, "allFalse"},
        new Object[]{true, "mixedOne"},
        new Object[]{true, "mixedOne"},
        new Object[]{false, "mixedOne"},
        new Object[]{false, "mixedTwo"},
        new Object[]{true, "mixedTwo"},
        new Object[]{false, "mixedTwo"},
        new Object[]{null, "withNulls"},
        new Object[]{true, "withNulls"},
        new Object[]{false, "withNulls"},
        new Object[]{null, "onlyNulls"},
    };

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (Object[] record : recordContents) {
      GenericRow genericRow = new GenericRow();
      genericRow.putValue(BOOLEAN_COLUMN, record[0]);
      genericRow.putValue(GROUP_BY_COLUMN, record[1]);
      records.add(genericRow);
    }

    ImmutableSegment immutableSegment = setUpSingleSegment(records, SEGMENT_NAME);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private ImmutableSegment setUpSingleSegment(List<GenericRow> recordSet, String segmentName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    segmentGeneratorConfig.setNullHandlingEnabled(true);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(recordSet));
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  protected <T extends Operator> T getOperator(String query, boolean enableNullHandling) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);

    pinotQuery.setQueryOptions(
        ImmutableMap.of(
            CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING,
            String.valueOf(enableNullHandling)));

    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(serverPinotQuery);
    return (T) PLAN_MAKER.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
  }

  @Test(dataProvider = "nullHandling")
  public void testBooleanAnd(boolean isNullHandlingEnabled) {
    // Given:
    String query = "SELECT BOOL_AND(boolColumn) FROM testTable GROUP BY groupByColumn";
    GroupByOperator operator = getOperator(query, isNullHandlingEnabled);

    // When:
    GroupByResultsBlock result = operator.nextBlock();

    // Then;
    QueriesTestUtils.testInnerSegmentExecutionStatistics(
        // multiply values by two because we copy the segments into two different segments
        operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregates = result.getAggregationGroupByResult();
    Iterator<GroupKeyGenerator.GroupKey> keys = aggregates.getGroupKeyIterator();
    while (keys.hasNext()) {
      GroupKeyGenerator.GroupKey key = keys.next();
      switch ((String) key._keys[0]) {
        case "allFalse":
        case "mixedOne":
        case "mixedTwo":
        case "withNulls":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), 0);
          break;
        case "allTrue":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), 1);
          break;
        case "onlyNulls":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), isNullHandlingEnabled ? null : 0);
          break;
        default:
          throw new IllegalStateException("Unexpected grouping: " + key._keys[0]);
      }
    }
  }

  @Test(dataProvider = "nullHandling")
  public void testBooleanOr(boolean isNullHandlingEnabled) {
    // Given:
    String query = "SELECT BOOL_OR(boolColumn) FROM testTable GROUP BY groupByColumn";
    GroupByOperator operator = getOperator(query, isNullHandlingEnabled);

    // When:
    GroupByResultsBlock result = operator.nextBlock();

    // Then;
    QueriesTestUtils.testInnerSegmentExecutionStatistics(
        // multiply values by two because we copy the segments into two different segments
        operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregates = result.getAggregationGroupByResult();
    Iterator<GroupKeyGenerator.GroupKey> keys = aggregates.getGroupKeyIterator();
    while (keys.hasNext()) {
      GroupKeyGenerator.GroupKey key = keys.next();
      switch ((String) key._keys[0]) {
        case "mixedOne":
        case "mixedTwo":
        case "allTrue":
        case "withNulls":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), 1);
          break;
        case "allFalse":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), 0);
          break;
        case "onlyNulls":
          Assert.assertEquals(aggregates.getResultForGroupId(0, key._groupId), isNullHandlingEnabled ? null : 0);
          break;
        default:
          throw new IllegalStateException("Unexpected grouping: " + key._keys[0]);
      }
    }
  }

  @Test(dataProvider = "nullHandling")
  public void testBooleanAndNoGroupBy(boolean isNullHandlingEnabled) {
    // Given:
    String query = "SELECT BOOL_AND(boolColumn) FROM testTable WHERE groupByColumn = 'allTrue'";
    AggregationOperator operator = getOperator(query, isNullHandlingEnabled);

    // When:
    AggregationResultsBlock result = operator.nextBlock();

    // Then;
    List<Object> aggregates = result.getResults();
    Assert.assertEquals(aggregates.size(), 1);
    Assert.assertEquals(aggregates.get(0), 1);
  }
}
