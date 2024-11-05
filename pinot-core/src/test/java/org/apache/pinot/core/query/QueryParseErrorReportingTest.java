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
package org.apache.pinot.core.query;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.PinotRuntimeException;
import org.apache.pinot.core.query.aggregation.function.AbstractAggregationFunctionTest;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.TestAggregationFunction;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentMatcher;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryParseErrorReportingTest extends AbstractAggregationFunctionTest {

  private static final String RAW_TABLE_NAME = "parkingData";

  private static final String IS_OCCUPIED_COLUMN = "isOccupied";
  private static final String LEVEL_ID_COLUMN = "levelId";
  private static final String LOT_ID_COLUMN = "lotId";
  private static final String EVENT_TIME_COLUMN = "eventTime";

  private static final DateTimeFormatSpec FORMATTER =
      new DateTimeFormatSpec("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS");

  @Test
  public void testNoKeyAggregateConversionError() {
    FluentQueryTest.withBaseDir(_baseDir)
                   .withNullHandling(false)
                   .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.STRING),
                       SINGLE_FIELD_TABLE_CONFIG)
                   .onFirstInstance(
                       new Object[]{"A"}
                   )
                   .andOnSecondInstance(
                       new Object[]{"1A"}
                   )
                   .whenQuery("select max(myField) from testTable")
                   .thenResultIsException(
                       "Error when processing testTable.myField.*NumberFormatException: For input string:", 2);
  }

  @Test
  public void testKeyedAggregateConversionError() {
    FluentQueryTest.withBaseDir(_baseDir)
                   .withNullHandling(false)
                   .givenTable(
                       new Schema.SchemaBuilder()
                           .setSchemaName("testTable")
                           .setEnableColumnBasedNullHandling(true)
                           .addDimensionField("key", FieldSpec.DataType.STRING)
                           .addDimensionField("value", FieldSpec.DataType.STRING)
                           .build(), SINGLE_FIELD_TABLE_CONFIG)
                   .onFirstInstance(
                       new Object[]{"k1", "v10"}
                   )
                   .andOnSecondInstance(
                       new Object[]{"k2", "20v"}
                   )
                   .whenQuery("select key, max(value) from testTable group by key")
                   .thenResultIsException(
                       "Error when processing testTable.value.*NumberFormatException: For input string:", 2);
  }

  @Test(dataProvider = "dataTypes")
  public void testGapFillConversionError(FieldSpec.DataType functionType, boolean nullHandlingEnabled) {
    String query = "SELECT "
        + " time_col, TEST_AGG( occupied, '" + functionType + "' ) as occupied_slots_count "
        + "FROM ( "
        + "  SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', '1636257600000',  '1636286400000', '1:HOURS',"
        + "                 FILL( occupied, 'FILL_PREVIOUS_VALUE'), "
        + "                 TIMESERIESON(levelId, lotId) ) AS time_col,"
        + "         occupied , lotId, levelId"
        + "  FROM ("
        + "    SELECT DATETRUNC('hour', eventTime, 'milliseconds') AS time_col, "
        + "           lastWithTime(isOccupied, eventTime, 'STRING') as occupied, "
        + "           lotId, levelId "
        + "    FROM parkingData "
        + "    GROUP BY time_col, levelId, lotId"
        + "    LIMIT 200 "
        + "  ) "
        + "  LIMIT 200 "
        + ") "
        + " GROUP BY time_col ";

    try (MockedStatic<AggregationFunctionFactory> mock = Mockito.mockStatic(AggregationFunctionFactory.class,
        Mockito.CALLS_REAL_METHODS)) {

      mock.when(() -> {
        AggregationFunctionFactory.getAggregationFunction(Mockito.argThat(new ArgumentMatcher<FunctionContext>() {
          @Override
          public boolean matches(FunctionContext argument) {
            return "testagg".equals(argument.getFunctionName());
          }
        }), Mockito.anyBoolean());
      }).thenAnswer(invocation -> {
            List<ExpressionContext> arguments = ((FunctionContext) invocation.getArgument(0)).getArguments();
            return new TestAggregationFunction(arguments.get(0), arguments.get(1), false);
          }
      );

      PinotRuntimeException pe = Assert.expectThrows(PinotRuntimeException.class, () -> {
        FluentQueryTest.withBaseDir(_baseDir)
                       .withNullHandling(true)
                       .givenTable(
                           new Schema.SchemaBuilder()
                               .setSchemaName(RAW_TABLE_NAME)
                               .setEnableColumnBasedNullHandling(nullHandlingEnabled)
                               .addSingleValueDimension(EVENT_TIME_COLUMN, FieldSpec.DataType.LONG)
                               .addDimensionField(IS_OCCUPIED_COLUMN, FieldSpec.DataType.STRING,
                                   f -> {
                                     f.setDefaultNullValue(null);
                                     f.setNullable(nullHandlingEnabled);
                                   })
                               .addSingleValueDimension(LEVEL_ID_COLUMN, FieldSpec.DataType.STRING)
                               .addSingleValueDimension(LOT_ID_COLUMN, FieldSpec.DataType.STRING)
                               .setPrimaryKeyColumns(Arrays.asList(LOT_ID_COLUMN, EVENT_TIME_COLUMN))
                               .build(),
                           new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build())
                       .onFirstInstance(
                           row("2021-11-07 04:11:00.000", 0, 0, "v1"),
                           row("2021-11-07 04:21:00.000", 0, 0, "true"),
                           row("2021-11-07 04:31:00.000", 1, 0, "1.2QQ"),
                           row("2021-11-07 04:31:00.000", 1, 0, null)
                       ).andOnSecondInstance(
                           row("2021-11-07 04:11:00.000", 0, 0, "v1"),
                           row("2021-11-07 04:21:00.000", 0, 0, "true"),
                           row("2021-11-07 04:31:00.000", 1, 0, "1.2QQ"),
                           row("2021-11-07 04:31:00.000", 1, 0, null)
                       )
                       .whenQuery(query); // exception is thrown during local merge
      });

      Assert.assertTrue(pe.getMessage().matches(
          ".*Error when processing parkingData.occupied.*NumberFormatException: .*"), pe.getMessage());
    }
  }

  @Test
  public void testFilteredAggregateConversionError() {
    FluentQueryTest.withBaseDir(_baseDir)
                   .withNullHandling(false)
                   .givenTable(
                       new Schema.SchemaBuilder()
                           .setSchemaName(RAW_TABLE_NAME)
                           .setEnableColumnBasedNullHandling(true)
                           .addSingleValueDimension(EVENT_TIME_COLUMN, FieldSpec.DataType.LONG)
                           .addDimensionField(IS_OCCUPIED_COLUMN, FieldSpec.DataType.STRING)
                           .addSingleValueDimension(LEVEL_ID_COLUMN, FieldSpec.DataType.STRING)
                           .addSingleValueDimension(LOT_ID_COLUMN, FieldSpec.DataType.STRING)
                           .setPrimaryKeyColumns(Arrays.asList(LOT_ID_COLUMN, EVENT_TIME_COLUMN))
                           .build(),
                       new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build())
                   .onFirstInstance(
                       row("2021-11-07 04:11:00.000", 0, 0, "v1"),
                       row("2021-11-07 04:21:00.000", 0, 0, "true"),
                       row("2021-11-07 04:31:00.000", 1, 0, "1.2QQ"),
                       row("2021-11-07 04:31:00.000", 1, 0, null)
                   )
                   .andOnSecondInstance(
                       row("2021-11-07 04:11:00.000", 0, 0, "v1"),
                       row("2021-11-07 04:21:00.000", 0, 0, "true"),
                       row("2021-11-07 04:31:00.000", 1, 0, "1.2QQ"),
                       row("2021-11-07 04:31:00.000", 1, 0, null)
                   )
                   .whenQuery(
                       // max with filter triggers `Cannot compute max for non-numeric type: STRING`
                       "select lastWithTime(isOccupied, eventTime, 'INT') filter (where levelId in ('level_0')) from "
                           + "parkingData where lotId = 'lot_0'")
                   .thenResultIsException(
                       "Error when processing parkingData.lastwithtime\\(isOccupied,eventTime,'INT'\\)"
                           + ".*NumberFormatException: For input string:", 2);
  }

  private static Object[] row(String timestamp, int level, int lot, String isOccupied) {
    return new Object[]{
        FORMATTER.fromFormatToMillis(timestamp),
        isOccupied,
        "level_" + level,
        "lot_" + lot,
    };
  }

  @DataProvider(name = "dataTypes")
  public Object[][] dataTypes() {
    FieldSpec.DataType[] types = {
        FieldSpec.DataType.FLOAT,
        FieldSpec.DataType.DOUBLE,
        FieldSpec.DataType.INT,
        FieldSpec.DataType.LONG,
        FieldSpec.DataType.BIG_DECIMAL
    };

    Object[][] result = new Object[types.length * 2][2];

    for (int i = 0; i < types.length; i++) {
      result[i * 2] = new Object[]{types[i], false};
      result[i * 2 + 1] = new Object[]{types[i], true};
    }

    return result;

//    FieldSpec.DataType[][] result = new FieldSpec.DataType[types.length * types.length][2];
//
//    for (int i = 0; i < types.length; i++) {
//      for (int j = 0; j < types.length; j++) {
//        {
//          if (types[j] == FieldSpec.DataType.BIG_DECIMAL) {
//            continue;
//          }
//          result[i * types.length + j] = new FieldSpec.DataType[]{types[i], types[j]};
//        }
//      }
//    }
//
//    return result;
//  }
  }
}
