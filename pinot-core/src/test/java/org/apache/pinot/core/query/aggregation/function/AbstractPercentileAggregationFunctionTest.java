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

import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class AbstractPercentileAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new Scenario(FieldSpec.DataType.INT),
        new Scenario(FieldSpec.DataType.LONG),
        new Scenario(FieldSpec.DataType.FLOAT),
        new Scenario(FieldSpec.DataType.DOUBLE),
    };
  }

  public abstract String callStr(String column, int percent);

  public String getFinalResultColumnType() {
    return "DOUBLE";
  }

  public class Scenario {
    private final FieldSpec.DataType _dataType;

    public Scenario(FieldSpec.DataType dataType) {
      _dataType = dataType;
    }

    public FieldSpec.DataType getDataType() {
      return _dataType;
    }

    public FluentQueryTest.DeclaringTable getDeclaringTable(boolean nullHandlingEnabled) {
      return givenSingleNullableFieldTable(_dataType, nullHandlingEnabled);
    }

    @Override
    public String toString() {
      return "Scenario{" + "dt=" + _dataType + '}';
    }
  }

  FluentQueryTest.TableWithSegments withDefaultData(Scenario scenario, boolean nullHandlingEnabled) {
    return scenario.getDeclaringTable(nullHandlingEnabled)
        .onFirstInstance("myField",
            "null",
            "0",
            "null",
            "1",
            "null",
            "2",
            "null",
            "3",
            "null",
            "4",
            "null"
        ).andSegment("myField",
            "null",
            "5",
            "null",
            "6",
            "null",
            "7",
            "null",
            "8",
            "null",
            "9",
            "null"
        );
  }

  String minValue(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT: return "-2.147483648E9";
      case LONG: return "-9.223372036854776E18";
      case FLOAT: return "-Infinity";
      case DOUBLE: return "-Infinity";
      default:
        throw new IllegalArgumentException("Unexpected type " + dataType);
    }
  }

  String expectedAggrWithoutNull10(Scenario scenario) {
    return minValue(scenario._dataType);
  }

  String expectedAggrWithoutNull15(Scenario scenario) {
    return minValue(scenario._dataType);
  }

  String expectedAggrWithoutNull30(Scenario scenario) {
    return minValue(scenario._dataType);
  }

  String expectedAggrWithoutNull35(Scenario scenario) {
    return minValue(scenario._dataType);
  }

  String expectedAggrWithoutNull50(Scenario scenario) {
    return minValue(scenario._dataType);
  }

  String expectedAggrWithoutNull55(Scenario scenario) {
    return "0";
  }

  String expectedAggrWithoutNull70(Scenario scenario) {
    return "3";
  }

  String expectedAggrWithoutNull75(Scenario scenario) {
    return "4";
  }

  String expectedAggrWithoutNull90(Scenario scenario) {
    return "7";
  }

  String expectedAggrWithoutNull100(Scenario scenario) {
    return "9";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithoutNull(Scenario scenario) {

    FluentQueryTest.TableWithSegments instance = withDefaultData(scenario, false);

    instance
        .whenQuery("select " + callStr("myField", 10) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull10(scenario));

    instance
        .whenQuery("select " + callStr("myField", 15) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull15(scenario));

    instance
        .whenQuery("select " + callStr("myField", 30) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull30(scenario));
    instance
        .whenQuery("select " + callStr("myField", 35) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull35(scenario));

    instance
        .whenQuery("select " + callStr("myField", 50) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull50(scenario));
    instance
        .whenQuery("select " + callStr("myField", 55) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull55(scenario));

    instance
        .whenQuery("select " + callStr("myField", 70) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull70(scenario));

    instance
        .whenQuery("select " + callStr("myField", 75) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull75(scenario));

    instance
        .whenQuery("select " + callStr("myField", 90) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull90(scenario));

    instance
        .whenQuery("select " + callStr("myField", 100) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithoutNull100(scenario));
  }

  String expectedAggrWithNull10(Scenario scenario) {
    return "1";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull10(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 10) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull10(scenario));
  }

  String expectedAggrWithNull15(Scenario scenario) {
    return "1";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull15(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 15) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull15(scenario));
  }

  String expectedAggrWithNull30(Scenario scenario) {
    return "3";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull30(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 30) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull30(scenario));
  }

  String expectedAggrWithNull35(Scenario scenario) {
    return "3";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull35(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 35) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull35(scenario));
  }

  String expectedAggrWithNull50(Scenario scenario) {
    return "5";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull50(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 50) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull50(scenario));
  }

  String expectedAggrWithNull55(Scenario scenario) {
    return "5";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull55(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 55) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull55(scenario));
  }

  String expectedAggrWithNull70(Scenario scenario) {
    return "7";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull70(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 70) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull70(scenario));
  }

  String expectedAggrWithNull75(Scenario scenario) {
    return "7";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull75(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 75) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull75(scenario));
  }

  String expectedAggrWithNull100(Scenario scenario) {
    return "9";
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull100(Scenario scenario) {
    withDefaultData(scenario, true)
        .whenQuery("select " + callStr("myField", 100) + " from testTable")
        .thenResultIs(getFinalResultColumnType(), expectedAggrWithNull100(scenario));
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andSegment("myField",
            "9"
        ).andSegment("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select $segmentName, " + callStr("myField", 50) + " from testTable "
            + "group by $segmentName order by $segmentName")
        .thenResultIs("STRING | " + getFinalResultColumnType(),
            "testTable_0 | " + minValue(scenario._dataType),
            "testTable_1 |  9",
            "testTable_2 | " + minValue(scenario._dataType)
        );
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andSegment("myField",
            "9"
        ).andSegment("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select $segmentName, " + callStr("myField", 50) + " from testTable "
            + "group by $segmentName order by $segmentName")
        .thenResultIs("STRING | " + getFinalResultColumnType(),
            "testTable_0 | 1",
            "testTable_1 | 9",
            "testTable_2 | null"
        );
  }
}
