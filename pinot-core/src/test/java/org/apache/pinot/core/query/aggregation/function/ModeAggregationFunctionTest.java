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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ModeAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new Scenario(FieldSpec.DataType.INT, true),

        new Scenario(FieldSpec.DataType.INT, false),
        new Scenario(FieldSpec.DataType.LONG, false),
        new Scenario(FieldSpec.DataType.FLOAT, false),
        new Scenario(FieldSpec.DataType.DOUBLE, false),
    };
  }

  public class Scenario {
    private final FieldSpec.DataType _dataType;
    private final boolean _dictionary;

    public Scenario(FieldSpec.DataType dataType, boolean dictionary) {
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

  String aggrWithoutNullResult(FieldSpec.DataType dt) {
    switch (dt) {
      case INT: return "-2.147483648E9";
      case LONG: return "-9.223372036854776E18";
      case FLOAT: return "-Infinity";
      case DOUBLE: return "-Infinity";
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
            "1",
            "null"
        )
        .whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", aggrWithoutNullResult(scenario._dataType));
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
            "1",
            "null"
        ).whenQuery("select mode(myField) as mode from testTable")
        .thenResultIs("DOUBLE", "1");
  }

  String aggrSvWithoutNullResult(FieldSpec.DataType dt) {
    switch (dt) {
      case INT: return "-2.147483648E9";
      case LONG: return "-9.223372036854776E18";
      case FLOAT: return "-Infinity";
      case DOUBLE: return "-Infinity";
      default: throw new IllegalArgumentException(dt.toString());
    }
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
            "1",
            "null"
        ).whenQuery("select 'cte', mode(myField) as mode from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | " + aggrSvWithoutNullResult(scenario._dataType));
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
            "1",
            "null"
        ).whenQuery("select 'cte', mode(myField) as mode from testTable group by 'cte'")
        .thenResultIs("STRING | DOUBLE", "cte | 1");
  }

  String aggrMvWithoutNullResult(FieldSpec.DataType dt) {
    switch (dt) {
      case INT: return "-2.147483648E9";
      case LONG: return "-9.223372036854776E18";
      case FLOAT: return "-Infinity";
      case DOUBLE: return "-Infinity";
      default: throw new IllegalArgumentException(dt.toString());
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "1",
            "null"
        ).whenQuery("select 'cte1' as cte1, 'cte2' as cte2, mode(myField) as mode from testTable group by cte1, cte2")
        .thenResultIs("STRING | STRING | DOUBLE", "cte1 | cte2 | " + aggrMvWithoutNullResult(scenario._dataType));
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "1",
            "null"
        ).whenQuery("select 'cte1' as cte1, 'cte2' as cte2, mode(myField) as mode from testTable group by cte1, cte2")
        .thenResultIs("STRING | STRING | DOUBLE", "cte1 | cte2 | 1");
  }
}
