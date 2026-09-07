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

package org.apache.pinot.core.query.aggregation.function;

import java.sql.Timestamp;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ModeAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new Scenario(DataType.INT, true),
        new Scenario(DataType.INT, false),
        new Scenario(DataType.LONG, false),
        new Scenario(DataType.FLOAT, false),
        new Scenario(DataType.DOUBLE, false),
    };
  }

  @DataProvider(name = "stringScenarios")
  Object[] stringScenarios() {
    return new Object[]{new Scenario(DataType.STRING, true), new Scenario(DataType.STRING, false)};
  }

  @DataProvider(name = "timestampScenarios")
  Object[] timestampScenarios() {
    return new Object[]{new Scenario(DataType.TIMESTAMP, true), new Scenario(DataType.TIMESTAMP, false)};
  }

  @Test(dataProvider = "stringScenarios")
  void stringModeMergesValueCountsAcrossSegments(Scenario scenario) {
    // Each server has a different local mode. The shared value wins after merging the counts.
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "apple", "apple", "apple", "banana", "banana", "null")
        .andSegment("myField", "banana")
        .andOnSecondInstance("myField", "cherry", "cherry", "cherry", "banana", "banana", "null")
        .whenQuery("select mode(myField, 'MIN', 'STRING') as mode from testTable")
        .thenResultTextIs("mode[STRING]\nbanana");
  }

  @Test(dataProvider = "stringScenarios")
  void stringModeTieReducers(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "zebra", "apple", "zebra", "banana", "null")
        .andOnSecondInstance("myField", "apple", "apple", "zebra", "null")
        .whenQuery("select mode(myField, 'MIN', 'STRING'), mode(myField, 'MAX', 'STRING') from testTable")
        .thenResultIs("STRING | STRING", "apple | zebra");
  }

  @Test(dataProvider = "stringScenarios")
  void stringModeWithNullAndComputedEmptyString(Scenario scenario) {
    // The CSV-backed fixture treats empty cells as null. Generate an actual empty string in the query instead.
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "empty", "empty", "apple")
        .andOnSecondInstance("myField", "null", "null", "apple")
        .whenQuery("select mode(case when myField = 'empty' then '' else myField end, 'MIN', 'STRING') as mode "
            + "from testTable")
        .thenResultIs(new Object[]{""})
        .whenQuery("select mode(case when myField = 'empty' then null else myField end, 'MIN', 'STRING') as mode "
            + "from testTable")
        .thenResultTextIs("mode[STRING]\napple")
        .whenQuery("select myField, mode(case when myField = 'empty' then '' else myField end, 'MIN', 'STRING') "
            + "from testTable group by myField order by myField")
        .thenResultIs(new Object[]{"apple", "apple"}, new Object[]{"empty", ""}, new Object[]{null, null});
  }

  @Test(dataProvider = "stringScenarios")
  void stringModeAllNullAndEmptyInput(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select mode(myField, 'MIN', 'STRING') as mode from testTable")
        .thenResultIs(new Object[]{null})
        .whenQuery("select mode(myField, 'MIN', 'STRING') as mode from testTable where myField = 'absent'")
        .thenResultIs(new Object[]{null})
        .whenQuery("select 'group', mode(myField, 'MIN', 'STRING') as mode from testTable group by 'group'")
        .thenResultIs("STRING | STRING", "group | null");
  }

  @Test(dataProvider = "stringScenarios")
  void stringModeWithoutNullHandling(Scenario scenario) {
    // Without null handling, three null defaults must beat the two ordinary values.
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "null", "null", "apple")
        .andOnSecondInstance("myField", "apple")
        .whenQuery("select mode(myField, 'MIN', 'STRING') as mode from testTable")
        .thenResultIs(new Object[]{"null"})
        .whenQuery("select mode(myField, 'MIN', 'STRING') as mode from testTable where myField = 'absent'")
        .thenResultIs(new Object[]{null});
  }

  @Test(dataProvider = "timestampScenarios")
  void timestampModePreservesTypeAndTieOrdering(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "2026-09-03 10:11:12.123", "2026-09-04 10:11:12.456", "null")
        .andOnSecondInstance("myField", "2026-09-04 10:11:12.456", "2026-09-03 10:11:12.123", "null")
        .whenQuery("select mode(myField, 'MIN', 'TIMESTAMP') as mode, mode(myField, 'MAX', 'TIMESTAMP') as latest "
            + "from testTable")
        .thenResultTextIs("mode[TIMESTAMP] | latest[TIMESTAMP]\n"
            + "2026-09-03 10:11:12.123 | 2026-09-04 10:11:12.456")
        .whenQuery("select myField, mode(myField, 'MIN', 'TIMESTAMP') as mode "
            + "from testTable group by myField order by myField")
        .thenResultIs(new Object[]{"2026-09-03 10:11:12.123", "2026-09-03 10:11:12.123"},
            new Object[]{"2026-09-04 10:11:12.456", "2026-09-04 10:11:12.456"}, new Object[]{null, null})
        .whenQuery("select fromTimestamp(mode(myField, 'MIN', 'TIMESTAMP')) as epochMillis from testTable")
        .thenResultIs(new Object[]{Timestamp.valueOf("2026-09-03 10:11:12.123").getTime()});
  }

  @Test(dataProvider = "timestampScenarios")
  void timestampModeAllNullAndEmptyInput(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select mode(myField, 'MIN', 'TIMESTAMP') as mode from testTable")
        .thenResultIs(new Object[]{null})
        .whenQuery("select mode(myField, 'MIN', 'TIMESTAMP') as mode "
            + "from testTable where myField > '2026-09-03 00:00:00'")
        .thenResultIs(new Object[]{null});
  }

  public class Scenario {
    private final DataType _dataType;
    private final boolean _dictionary;

    public Scenario(DataType dataType, boolean dictionary) {
      _dataType = dataType;
      _dictionary = dictionary;
    }

    public FluentQueryTest.DeclaringTable getDeclaringTable(boolean nullHandlingEnabled) {
      FieldConfig.EncodingType encodingType =
          _dictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW;
      return givenSingleNullableFieldTable(_dataType, nullHandlingEnabled, builder -> {
        builder.withEncodingType(encodingType);
        builder.withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH);
      });
    }

    @Override
    public String toString() {
      return "Scenario{" + "dt=" + _dataType + ", dict=" + _dictionary + '}';
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrWithoutNullAndEmptySegments(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null", "null")
        .whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", aggrWithoutNullResult(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNullAndEmptySegments(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null", "null")
        .whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  String aggrWithoutNullResult(DataType dt) {
    switch (dt) {
      case INT:
        return "-2.147483648E9";
      case LONG:
        return "-9.223372036854776E18";
      case FLOAT:
        return "-Infinity";
      case DOUBLE:
        return "-Infinity";
      default:
        throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", aggrWithoutNullResult(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", "1");
  }

  String aggrSvWithoutNullResult(DataType dt) {
    switch (dt) {
      case INT:
        return "-2.147483648E9";
      case LONG:
        return "-9.223372036854776E18";
      case FLOAT:
        return "-Infinity";
      case DOUBLE:
        return "-Infinity";
      default:
        throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select 'cte', mode(myField) as mode from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | " + aggrSvWithoutNullResult(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select 'cte', mode(myField) as mode from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | 1");
  }

  @Test(dataProvider = "scenarios")
  void aggrSvSelfWithoutNull(Scenario scenario) {
    PinotDataType pinotDataType = PinotDataType.valueOf(scenario._dataType.name());

    Object defaultNullValue;
    switch (scenario._dataType) {
      case INT:
        defaultNullValue = Integer.MIN_VALUE;
        break;
      case LONG:
        defaultNullValue = Long.MIN_VALUE;
        break;
      case FLOAT:
        defaultNullValue = Float.NEGATIVE_INFINITY;
        break;
      case DOUBLE:
        defaultNullValue = Double.NEGATIVE_INFINITY;
        break;
      default:
        throw new IllegalArgumentException("Unexpected scenario data type " + scenario._dataType);
    }

    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1", "2")
        .andOnSecondInstance("myField", "null", "1", "2")
        .whenQuery("select myField, mode(myField) as mode from testTable group by myField order by myField")
        .thenResultIs(
            pinotDataType + " | DOUBLE",
            defaultNullValue + " | " + aggrSvWithoutNullResult(scenario._dataType),
            "1           | 1",
            "2           | 2"
        );
  }

  @Test(dataProvider = "scenarios")
  void aggrSvSelfWithNull(Scenario scenario) {
    PinotDataType pinotDataType = PinotDataType.valueOf(scenario._dataType.name());

    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1", "2")
        .andOnSecondInstance("myField", "null", "1", "2")
        .whenQuery("select myField, mode(myField) as mode from testTable group by myField order by myField")
        .thenResultIs(
            pinotDataType + " | DOUBLE",
            "1 | 1",
            "2 | 2",
            "null | null"
        );
  }

  String aggrMvWithoutNullResult(DataType dt) {
    switch (dt) {
      case INT:
        return "-2.147483648E9";
      case LONG:
        return "-9.223372036854776E18";
      case FLOAT:
        return "-Infinity";
      case DOUBLE:
        return "-Infinity";
      default:
        throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithoutNull(Scenario scenario) {
    // TODO: This test is not actually exercising aggregateGroupByMV
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select 'cte1' as cte1, 'cte2' as cte2, mode(myField) as mode from testTable group by cte1, cte2")
        .thenResultIs("STRING | STRING | DOUBLE", "cte1 | cte2 | " + aggrMvWithoutNullResult(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithNull(Scenario scenario) {
    // TODO: This test is not actually exercising aggregateGroupByMV
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1", "null")
        .andOnSecondInstance("myField", "null", "1", "null")
        .whenQuery("select 'cte1' as cte1, 'cte2' as cte2, mode(myField) as mode from testTable group by cte1, cte2")
        .thenResultIs("STRING | STRING | DOUBLE", "cte1 | cte2 | 1");
  }
}
