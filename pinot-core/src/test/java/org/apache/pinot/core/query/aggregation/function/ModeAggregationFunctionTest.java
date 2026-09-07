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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


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

  @DataProvider
  Object[] typedScenarios() {
    return new Object[]{new Scenario(DataType.STRING, true), new Scenario(DataType.STRING, false),
        new Scenario(DataType.TIMESTAMP, true), new Scenario(DataType.TIMESTAMP, false)};
  }

  @Test(dataProvider = "typedScenarios")
  void typedModeMergesCountsAndHandlesNulls(Scenario scenario) {
    String type = scenario._dataType.name();
    String first = scenario._dataType == DataType.STRING ? "apple" : "2026-09-03 10:11:12.123";
    String shared = scenario._dataType == DataType.STRING ? "banana" : "2026-09-04 10:11:12.456";
    String last = scenario._dataType == DataType.STRING ? "cherry" : "2026-09-05 10:11:12.789";
    // Each instance has a different local mode; the shared value wins only after merging full counts.
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", first, first, first, shared, shared, "null")
        .andOnSecondInstance("myField", last, last, last, shared, shared, "null")
        .whenQuery("select mode(myField, 'MIN', '" + type + "') as mode from testTable")
        .thenResultTextIs("mode[" + type + "]\n" + shared)
        .whenQuery("select mode(myField, 'MIN', '" + type + "') from testTable where myField is null")
        .thenResultIs(new Object[]{null})
        .whenQuery("select myField, mode(myField, 'MIN', '" + type + "') from testTable "
            + "group by myField order by myField")
        .thenResultIs(new Object[]{first, first}, new Object[]{shared, shared}, new Object[]{last, last},
            new Object[]{null, null});
  }

  @DataProvider
  Object[][] typedStates() {
    return new Object[][]{{"STRING", "", "zebra"}, {"TIMESTAMP", 9_007_199_254_740_992L, 9_007_199_254_740_993L}};
  }

  @Test(dataProvider = "typedStates")
  void typedModePreservesSerializedValuesAndTieReducers(String type, Object smaller, Object larger) {
    ModeAggregationFunction min = typedMode(type, "MIN");
    AggregationFunction.SerializedIntermediateResult serialized =
        min.serializeIntermediateResult(Map.of(smaller, 2L, larger, 1L));
    Map<?, Long> counts = min.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    counts = min.merge(counts, Map.of(larger, 1L));
    assertEquals(min.getFinalResultColumnType(), ColumnDataType.valueOf(type));
    assertEquals(min.extractFinalResult(counts), smaller);
    assertEquals(typedMode(type, "MAX").extractFinalResult(counts), larger);
    assertNull(min.extractFinalResult(null));
    expectThrows(IllegalArgumentException.class, () -> typedMode(type, "AVG"));
  }

  private static ModeAggregationFunction typedMode(String type, String reducer) {
    return new ModeAggregationFunction(List.of(ExpressionContext.forIdentifier("value"),
        ExpressionContext.forLiteral(Literal.stringValue(reducer)),
        ExpressionContext.forLiteral(Literal.stringValue(type))), true);
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
