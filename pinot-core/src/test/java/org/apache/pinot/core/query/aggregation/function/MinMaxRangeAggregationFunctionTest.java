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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MinMaxRangeAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new Scenario(FieldSpec.DataType.INT),
        new Scenario(FieldSpec.DataType.LONG),
        new Scenario(FieldSpec.DataType.FLOAT),
        new Scenario(FieldSpec.DataType.DOUBLE),
    };
  }

  public class Scenario {
    private final FieldSpec.DataType _dataType;

    public Scenario(FieldSpec.DataType dataType) {
      _dataType = dataType;
    }

    public FluentQueryTest.DeclaringTable getDeclaringTable(boolean nullHandlingEnabled) {
      return givenSingleNullableFieldTable(_dataType, nullHandlingEnabled);
    }

    @Override
    public String toString() {
      return "Scenario{" + "dt=" + _dataType + '}';
    }
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
  void aggrWithoutNull(Scenario scenario) {
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
        .thenResultIs("DOUBLE", diffBetweenMinAnd9(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull(Scenario scenario) {
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
  void aggrSvWithoutNull(Scenario scenario) {
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
        .thenResultIs("STRING | DOUBLE", "cte | " + diffBetweenMinAnd9(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithNull(Scenario scenario) {
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
  void aggrSvSelfWithoutNull(Scenario scenario) {
    PinotDataType pinotDataType = scenario._dataType == FieldSpec.DataType.INT
        ? PinotDataType.INTEGER : PinotDataType.valueOf(scenario._dataType.name());

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
            defaultNullValue + " | " + aggrSvSelfWithoutNullResult(scenario._dataType),
            "1                   | 0",
            "2                   | 0");
  }

  @Test(dataProvider = "scenarios")
  void aggrSvSelfWithNull(Scenario scenario) {
    PinotDataType pinotDataType = scenario._dataType == FieldSpec.DataType.INT
        ? PinotDataType.INTEGER : PinotDataType.valueOf(scenario._dataType.name());

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
}
