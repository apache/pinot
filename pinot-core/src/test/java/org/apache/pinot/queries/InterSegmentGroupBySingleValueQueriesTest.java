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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests order by queries
 */
public class InterSegmentGroupBySingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final InstancePlanMakerImplV2 TRIM_ENABLED_PLAN_MAKER =
      new InstancePlanMakerImplV2(InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, 1,
          InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE,
          InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);

  @Test(dataProvider = "groupByOrderByDataProvider")
  public void testGroupByOrderBy(String query, long expectedNumEntriesScannedPostFilter,
      ResultTable expectedResultTable) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), 120000L, 0L, expectedNumEntriesScannedPostFilter,
        120000L, expectedResultTable);
  }

  @Test(dataProvider = "groupByOrderByDataProvider")
  public void testGroupByOrderByWithTrim(String query, long expectedNumEntriesScannedPostFilter,
      ResultTable expectedResultTable) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query, TRIM_ENABLED_PLAN_MAKER), 120000L, 0L,
        expectedNumEntriesScannedPostFilter, 120000L, expectedResultTable);
  }

  /**
   * Provides various combinations of order by in ResultTable.
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider
  public Object[][] groupByOrderByDataProvider() {
    List<Object[]> entries = new ArrayList<>();

    // order by one of the group by columns
    String query = "SELECT column11, SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    DataSchema dataSchema = new DataSchema(new String[]{"column11", "sum(column1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<Object[]> results = Arrays.asList(new Object[]{"", 5935285005452.0}, new Object[]{"P", 88832999206836.0},
        new Object[]{"gFuH", 63202785888.0}, new Object[]{"o", 18105331533948.0}, new Object[]{"t", 16331923219264.0});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // order by one of the group by columns DESC
    query = "SELECT column11, sum(column1) FROM testTable GROUP BY column11 ORDER BY column11 DESC";
    results = new ArrayList<>(results);
    Collections.reverse(results);
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // order by one of the group by columns, LIMIT less than default
    query = "SELECT column11, Sum(column1) FROM testTable GROUP BY column11 ORDER BY column11 LIMIT 3";
    results = new ArrayList<>(results);
    Collections.reverse(results);
    results = results.subList(0, 3);
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // group by 2 dimensions, order by both, tie breaker
    query = "SELECT column11, column12, SUM(column1) FROM testTable"
        + " GROUP BY column11, column12 ORDER BY column11, column12";
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.DOUBLE
    });
    results = Arrays.asList(new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"", "dJWwFk", 55470665124.0000},
        new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0}, new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0},
        new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0}, new Object[]{"P", "XcBNHe", 120021767504.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // group by 2 columns, order by both, LIMIT more than default
    query = "SELECT column11, column12, SUM(column1) FROM testTable"
        + " GROUP BY column11, column12 ORDER BY column11, column12 LIMIT 15";
    results = new ArrayList<>(results);
    results.add(new Object[]{"P", "dJWwFk", 6224665921376.0});
    results.add(new Object[]{"P", "fykKFqiw", 1574451324140.0});
    results.add(new Object[]{"P", "gFuH", 860077643636.0});
    results.add(new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0});
    results.add(new Object[]{"gFuH", "HEuxNvH", 29872400856.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // group by 2 columns, order by both, one of them DESC
    query = "SELECT column11, column12, SUM(column1) FROM testTable"
        + " GROUP BY column11, column12 ORDER BY column11, column12 DESC";
    results = Arrays.asList(new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0},
        new Object[]{"", "dJWwFk", 55470665124.0000}, new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"P", "dJWwFk", 6224665921376.0},
        new Object[]{"P", "XcBNHe", 120021767504.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // order by group by column and an aggregation
    query = "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, sum"
        + "(column1)";
    results = Arrays.asList(new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0},
        new Object[]{"", "dJWwFk", 55470665124.0000}, new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"P", "XcBNHe", 120021767504.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0},
        new Object[]{"P", "dJWwFk", 6224665921376.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // order by only aggregation, DESC, LIMIT
    query = "SELECT column11, column12, SUM(column1) FROM testTable"
        + " GROUP BY column11, column12 ORDER BY SUM(column1) DESC LIMIT 50";
    results = Arrays.asList(new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0},
        new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0},
        new Object[]{"o", "MaztCmmxxgguBUxPti", 6905624581072.0}, new Object[]{"P", "dJWwFk", 6224665921376.0},
        new Object[]{"o", "HEuxNvH", 5026384681784.0}, new Object[]{"t", "MaztCmmxxgguBUxPti", 4492405624940.0},
        new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0}, new Object[]{"t", "HEuxNvH", 4424489490364.0},
        new Object[]{"o", "KrNxpdycSiwoRohEiTIlLqDHnx", 4051812250524.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"t", "KrNxpdycSiwoRohEiTIlLqDHnx", 3529048341192.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"t", "dJWwFk", 1349058948804.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"o", "dJWwFk", 1152689463360.0},
        new Object[]{"t", "oZgnrlDEtjjVpUoFLol", 1039101333316.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"o", "oZgnrlDEtjjVpUoFLol", 699381633640.0}, new Object[]{"t", "TTltMtFiRqUjvOG", 675238030848.0},
        new Object[]{"t", "fykKFqiw", 480973878052.0}, new Object[]{"t", "gFuH", 330331507792.0},
        new Object[]{"o", "TTltMtFiRqUjvOG", 203835153352.0}, new Object[]{"P", "XcBNHe", 120021767504.0},
        new Object[]{"o", "fykKFqiw", 62975165296.0}, new Object[]{"", "dJWwFk", 55470665124.0000},
        new Object[]{"gFuH", "HEuxNvH", 29872400856.0}, new Object[]{"gFuH", "MaztCmmxxgguBUxPti", 29170832184.0},
        new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0}, new Object[]{"t", "XcBNHe", 11276063956.0},
        new Object[]{"gFuH", "KrNxpdycSiwoRohEiTIlLqDHnx", 4159552848.0}, new Object[]{"o", "gFuH", 2628604920.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // multiple aggregations (group-by column not in select)
    query = "SELECT sum(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    dataSchema = new DataSchema(new String[]{"sum(column1)", "min(column6)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    results = Arrays.asList(new Object[]{5935285005452.0, 2.96467636E8}, new Object[]{88832999206836.0, 1689277.0},
        new Object[]{63202785888.0, 2.96467636E8}, new Object[]{18105331533948.0, 2.96467636E8},
        new Object[]{16331923219264.0, 1980174.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // order by aggregation with space/tab in order by
    query = "SELECT column11, column12, SUM(column1) FROM testTable"
        + " GROUP BY column11, column12 ORDER BY SUM  (\tcolumn1) DESC LIMIT 3";
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.DOUBLE
    });
    results = Arrays.asList(new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // order by an aggregation DESC, and group by column
    query = "SELECT column12, MIN(column6) FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, column12";
    dataSchema = new DataSchema(new String[]{"column12", "min(column6)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    results = Arrays.asList(new Object[]{"XcBNHe", 329467557.0}, new Object[]{"fykKFqiw", 296467636.0},
        new Object[]{"gFuH", 296467636.0}, new Object[]{"HEuxNvH", 6043515.0},
        new Object[]{"MaztCmmxxgguBUxPti", 6043515.0}, new Object[]{"dJWwFk", 6043515.0},
        new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 1980174.0}, new Object[]{"TTltMtFiRqUjvOG", 1980174.0},
        new Object[]{"oZgnrlDEtjjVpUoFLol", 1689277.0});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // aggregation in order-by but not in select
    query = "SELECT column12 FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, column12";
    dataSchema = new DataSchema(new String[]{"column12"}, new ColumnDataType[]{ColumnDataType.STRING});
    results =
        Arrays.asList(new Object[]{"XcBNHe"}, new Object[]{"fykKFqiw"}, new Object[]{"gFuH"}, new Object[]{"HEuxNvH"},
            new Object[]{"MaztCmmxxgguBUxPti"}, new Object[]{"dJWwFk"}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx"},
            new Object[]{"TTltMtFiRqUjvOG"}, new Object[]{"oZgnrlDEtjjVpUoFLol"});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // multiple aggregations in order-by but not in select
    query = "SELECT column12 FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, SUM(column1) LIMIT 3";
    dataSchema = new DataSchema(new String[]{"column12"}, new ColumnDataType[]{ColumnDataType.STRING});
    results = Arrays.asList(new Object[]{"XcBNHe"}, new Object[]{"gFuH"}, new Object[]{"fykKFqiw"});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // multiple aggregations in order-by, some in select
    query = "SELECT column12, MIN(column6) FROM testTable"
        + " GROUP BY column12 ORDER BY Min(column6) DESC, SUM(column1) LIMIT 3";
    dataSchema = new DataSchema(new String[]{"column12", "min(column6)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    results = Arrays.asList(new Object[]{"XcBNHe", 329467557.0}, new Object[]{"gFuH", 296467636.0},
        new Object[]{"fykKFqiw", 296467636.0});
    entries.add(new Object[]{query, 360000L, new ResultTable(dataSchema, results)});

    // numeric dimension should follow numeric ordering
    query = "select column17, count(*) from testTable group by column17 order by column17 limit 15";
    dataSchema = new DataSchema(new String[]{"column17", "count(*)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    results =
        Arrays.asList(new Object[]{83386499, 2924L}, new Object[]{217787432, 3892L}, new Object[]{227908817, 6564L},
            new Object[]{402773817, 7304L}, new Object[]{423049234, 6556L}, new Object[]{561673250, 7420L},
            new Object[]{635942547, 3308L}, new Object[]{638936844, 3816L}, new Object[]{939479517, 3116L},
            new Object[]{984091268, 3824L}, new Object[]{1230252339, 5620L}, new Object[]{1284373442, 7428L},
            new Object[]{1555255521, 2900L}, new Object[]{1618904660, 2744L}, new Object[]{1670085862, 3388L});
    entries.add(new Object[]{query, 120000L, new ResultTable(dataSchema, results)});

    // group by UDF order by UDF
    query = "SELECT sub(column1, 100000), COUNT(*) FROM testTable"
        + " GROUP BY sub(column1, 100000) ORDER BY sub(column1, 100000) LIMIT 3";
    dataSchema = new DataSchema(new String[]{"sub(column1,100000)", "count(*)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG});
    results = Arrays.asList(new Object[]{140528.0, 28L}, new Object[]{194355.0, 12L}, new Object[]{532157.0, 12L});
    entries.add(new Object[]{query, 120000L, new ResultTable(dataSchema, results)});

    // space/tab in UDF
    query =
        "SELECT sub(column1, 100000), COUNT(*) FROM testTable GROUP BY sub(column1, 100000) ORDER BY SUB(   column1, "
            + "100000\t) LIMIT 3";
    dataSchema = new DataSchema(new String[]{"sub(column1,100000)", "count(*)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG});
    results = Arrays.asList(new Object[]{140528.0, 28L}, new Object[]{194355.0, 12L}, new Object[]{532157.0, 12L});
    entries.add(new Object[]{query, 120000L, new ResultTable(dataSchema, results)});

    // Object type aggregation - comparable intermediate results (AVG, MINMAXRANGE)
    query = "SELECT column11, AVG(column6) FROM testTable GROUP BY column11  ORDER BY column11";
    dataSchema = new DataSchema(new String[]{"column11", "avg(column6)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    results = Arrays.asList(new Object[]{"", 296467636.0}, new Object[]{"P", 909380310.3521485},
        new Object[]{"gFuH", 296467636.0}, new Object[]{"o", 296467636.0}, new Object[]{"t", 526245333.3900426});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    query = "SELECT column11, AVG(column6) FROM testTable GROUP BY column11 ORDER BY AVG(column6), column11 DESC";
    dataSchema = new DataSchema(new String[]{"column11", "avg(column6)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    results =
        Arrays.asList(new Object[]{"o", 296467636.0}, new Object[]{"gFuH", 296467636.0}, new Object[]{"", 296467636.0},
            new Object[]{"t", 526245333.3900426}, new Object[]{"P", 909380310.3521485});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // Object type aggregation - non comparable intermediate results (DISTINCTCOUNT)
    query = "SELECT column12, DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY column12";
    dataSchema = new DataSchema(new String[]{"column12", "distinctcount(column11)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    results = Arrays.asList(new Object[]{"HEuxNvH", 5}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5},
        new Object[]{"MaztCmmxxgguBUxPti", 5}, new Object[]{"TTltMtFiRqUjvOG", 3}, new Object[]{"XcBNHe", 2},
        new Object[]{"dJWwFk", 4}, new Object[]{"fykKFqiw", 3}, new Object[]{"gFuH", 3},
        new Object[]{"oZgnrlDEtjjVpUoFLol", 4});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    query = "SELECT column12, DISTINCTCOUNT(column11) FROM testTable"
        + " GROUP BY column12 ORDER BY DistinctCount(column11), column12 DESC";
    dataSchema = new DataSchema(new String[]{"column12", "distinctcount(column11)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    results = Arrays.asList(new Object[]{"XcBNHe", 2}, new Object[]{"gFuH", 3}, new Object[]{"fykKFqiw", 3},
        new Object[]{"TTltMtFiRqUjvOG", 3}, new Object[]{"oZgnrlDEtjjVpUoFLol", 4}, new Object[]{"dJWwFk", 4},
        new Object[]{"MaztCmmxxgguBUxPti", 5}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5},
        new Object[]{"HEuxNvH", 5});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    // percentile
    query = "SELECT column11, percentile90(column6) FROM testTable"
        + " GROUP BY column11 ORDER BY PERCENTILE90(column6), column11 LIMIT 3";
    results = Arrays.asList(new Object[]{"", 2.96467636E8}, new Object[]{"gFuH", 2.96467636E8},
        new Object[]{"o", 2.96467636E8});
    dataSchema = new DataSchema(new String[]{"column11", "percentile90(column6)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    entries.add(new Object[]{query, 240000L, new ResultTable(dataSchema, results)});

    return entries.toArray(new Object[0][]);
  }
}
