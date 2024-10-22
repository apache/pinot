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

import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MinMaxRangeAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(FieldSpec.DataType.INT),
        new DataTypeScenario(FieldSpec.DataType.LONG),
        new DataTypeScenario(FieldSpec.DataType.FLOAT),
        new DataTypeScenario(FieldSpec.DataType.DOUBLE),
    };
  }

  String diffBetweenMinAnd9(FieldSpec.DataType dt) {
    switch (dt) {
      case INT: return "2.147483657E9";
      case LONG: return "9.223372036854776E18";
      case FLOAT: return "Infinity";
      case DOUBLE: return "Infinity";
      default: throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrWithoutNull(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "9",
            "null"
        )
        .whenQuery("select minmaxrange(myField) from testTable")
        .thenResultIs("DOUBLE", diffBetweenMinAnd9(scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "9",
            "null"
        ).whenQuery("select minmaxrange(myField) from testTable")
        .thenResultIs("DOUBLE", "8");
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithoutNull(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "9",
            "null"
        ).whenQuery("select 'cte', minmaxrange(myField) from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | " + diffBetweenMinAnd9(scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithNull(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "9",
            "null"
        ).whenQuery("select 'cte', minmaxrange(myField) from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | 8");
  }

  String aggrSvSelfWithoutNullResult(FieldSpec.DataType dt) {
    switch (dt) {
      case INT: return "0";
      case LONG: return "0";
      case FLOAT: return "NaN";
      case DOUBLE: return "NaN";
      default: throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrSvSelfWithoutNull(DataTypeScenario scenario) {
    PinotDataType pinotDataType = scenario.getDataType() == FieldSpec.DataType.INT
        ? PinotDataType.INTEGER : PinotDataType.valueOf(scenario.getDataType().name());

    Object defaultNullValue;
    switch (scenario.getDataType()) {
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
        throw new IllegalArgumentException("Unexpected scenario data type " + scenario.getDataType());
    }

    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "1",
            "2"
        ).whenQuery("select myField, minmaxrange(myField) from testTable group by myField order by myField")
        .thenResultIs(pinotDataType + " | DOUBLE",
            defaultNullValue + " | " + aggrSvSelfWithoutNullResult(scenario.getDataType()),
            "1                   | 0",
            "2                   | 0");
  }

  @Test(dataProvider = "scenarios")
  void aggrSvSelfWithNull(DataTypeScenario scenario) {
    PinotDataType pinotDataType = scenario.getDataType() == FieldSpec.DataType.INT
        ? PinotDataType.INTEGER : PinotDataType.valueOf(scenario.getDataType().name());

    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "1",
            "2"
        ).whenQuery("select myField, minmaxrange(myField) from testTable group by myField order by myField")
        .thenResultIs(pinotDataType + " | DOUBLE", "1 | 0", "2 | 0", "null | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupByMV(DataTypeScenario scenario) {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addMetricField("value", scenario.getDataType())
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 1},
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 2},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, minmaxrange(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1",
            "tag2    | 2",
            "tag3    | 0"
        )
        .whenQueryWithNullHandlingEnabled("select tags, minmaxrange(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1",
            "tag2    | 1",
            "tag3    | null"
        );
  }
}
