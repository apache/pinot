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

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


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

  @DataProvider(name = "nonNumericScenarios")
  Object[] nonNumericScenarios() {
    long earlier = 1_600_000_000_000L;
    long later = 1_700_000_000_000L;
    return new Object[]{
        new NonNumericScenario(DataType.STRING, true, "a", "b", "a", "b", "null"),
        new NonNumericScenario(DataType.STRING, false, "a", "b", "a", "b", "null"),
        new NonNumericScenario(DataType.BOOLEAN, true, "false", "true", false, true, false),
        new NonNumericScenario(DataType.BOOLEAN, false, "false", "true", false, true, false),
        new NonNumericScenario(DataType.TIMESTAMP, true, String.valueOf(earlier), String.valueOf(later),
            new Timestamp(earlier).toString(), new Timestamp(later).toString(), new Timestamp(0).toString()),
        new NonNumericScenario(DataType.TIMESTAMP, false, String.valueOf(earlier), String.valueOf(later),
            new Timestamp(earlier).toString(), new Timestamp(later).toString(), new Timestamp(0).toString())
    };
  }

  /// Inputs are the lower and the higher value; results are as the broker returns them.
  public class NonNumericScenario {
    private final DataType _dataType;
    private final boolean _dictionary;
    private final String _low;
    private final String _high;
    private final Object _lowResult;
    private final Object _highResult;
    private final Object _defaultNullValueResult;

    public NonNumericScenario(DataType dataType, boolean dictionary, String low, String high, Object lowResult,
        Object highResult, Object defaultNullValueResult) {
      _dataType = dataType;
      _dictionary = dictionary;
      _low = low;
      _high = high;
      _lowResult = lowResult;
      _highResult = highResult;
      _defaultNullValueResult = defaultNullValueResult;
    }

    public FluentQueryTest.DeclaringTable getDeclaringTable(boolean nullHandlingEnabled) {
      FieldConfig.EncodingType encodingType =
          _dictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW;
      return givenSingleNullableFieldTable(_dataType, nullHandlingEnabled, builder -> {
        builder.withEncodingType(encodingType);
        builder.withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH);
      });
    }

    public String mode(String reducer) {
      return "mode(myField, '" + reducer + "', '" + _dataType + "')";
    }

    @Override
    public String toString() {
      return "NonNumericScenario{" + "dt=" + _dataType + ", dict=" + _dictionary + '}';
    }
  }

  @Test(dataProvider = "nonNumericScenarios")
  void nonNumericAggrWithNull(NonNumericScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", scenario._low, scenario._high)
        .andOnSecondInstance("myField", "null", scenario._high, "null")
        .whenQuery("select " + scenario.mode("MIN") + " as mode from testTable")
        .thenResultIs(new Object[]{scenario._highResult})
        .whenQuery("select 'cte', " + scenario.mode("MIN") + " as mode from testTable group by 'cte'")
        .thenResultIs(new Object[]{"cte", scenario._highResult});
  }

  @Test(dataProvider = "nonNumericScenarios")
  void nonNumericAggrWithoutNull(NonNumericScenario scenario) {
    // Nulls are read as the default null value, which becomes the most frequent value
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", scenario._low, scenario._high)
        .andOnSecondInstance("myField", "null", scenario._high, "null")
        .whenQuery("select " + scenario.mode("MIN") + " as mode from testTable")
        .thenResultIs(new Object[]{scenario._defaultNullValueResult})
        .whenQuery("select 'cte', " + scenario.mode("MIN") + " as mode from testTable group by 'cte'")
        .thenResultIs(new Object[]{"cte", scenario._defaultNullValueResult});
  }

  @Test(dataProvider = "nonNumericScenarios")
  void nonNumericAggrOnlyNulls(NonNumericScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null", "null")
        .whenQuery("select " + scenario.mode("MIN") + " as mode from testTable")
        .thenResultIs(new Object[]{null})
        .whenQuery("select 'cte', " + scenario.mode("MAX") + " as mode from testTable group by 'cte'")
        .thenResultIs(new Object[]{"cte", null});
  }

  @Test(dataProvider = "nonNumericScenarios")
  void nonNumericAggrTieReducers(NonNumericScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", scenario._high, scenario._low)
        .andOnSecondInstance("myField", scenario._low, scenario._high)
        .whenQuery("select " + scenario.mode("MIN") + ", " + scenario.mode("MAX") + " from testTable")
        .thenResultIs(new Object[]{scenario._lowResult, scenario._highResult})
        .whenQuery("select 'cte', " + scenario.mode("MIN") + ", " + scenario.mode("MAX")
            + " from testTable group by 'cte'")
        .thenResultIs(new Object[]{"cte", scenario._lowResult, scenario._highResult});
  }

  @Test
  void nonNumericSerializedMerge() {
    ModeAggregationFunction stringMode = createFunction("MAX", "STRING");
    assertEquals(stringMode.getFinalResultColumnType(), ColumnDataType.STRING);
    Object2LongOpenHashMap<String> strings1 = new Object2LongOpenHashMap<>(Map.of("a", 2L, "b", 1L));
    Object2LongOpenHashMap<String> strings2 = new Object2LongOpenHashMap<>(Map.of("b", 1L, "c", 2L));
    Map<?, Long> mergedStrings = stringMode.merge(roundTrip(stringMode, strings1), roundTrip(stringMode, strings2));
    assertEquals(mergedStrings, Map.of("a", 2L, "b", 2L, "c", 2L));
    assertEquals(stringMode.extractFinalResult(mergedStrings), "c");
    assertEquals(createFunction("MIN", "STRING").extractFinalResult(mergedStrings), "a");

    ModeAggregationFunction booleanMode = createFunction("MIN", "BOOLEAN");
    assertEquals(booleanMode.getFinalResultColumnType(), ColumnDataType.BOOLEAN);
    Int2LongOpenHashMap booleans = new Int2LongOpenHashMap(Map.of(0, 3L, 1, 3L));
    assertEquals(booleanMode.extractFinalResult(roundTrip(booleanMode, booleans)), 0);
    assertEquals(createFunction("MAX", "BOOLEAN").extractFinalResult(booleans), 1);

    ModeAggregationFunction timestampMode = createFunction("MAX", "TIMESTAMP");
    assertEquals(timestampMode.getFinalResultColumnType(), ColumnDataType.TIMESTAMP);
    Long2LongOpenHashMap timestamps1 = new Long2LongOpenHashMap(Map.of(Long.MAX_VALUE - 1, 2L, 5L, 1L));
    Long2LongOpenHashMap timestamps2 = new Long2LongOpenHashMap(Map.of(Long.MAX_VALUE, 2L));
    Map<?, Long> mergedTimestamps =
        timestampMode.merge(roundTrip(timestampMode, timestamps1), roundTrip(timestampMode, timestamps2));
    // The two modes are equal as doubles, so they must be compared as longs
    assertEquals(timestampMode.extractFinalResult(mergedTimestamps), Long.MAX_VALUE);
    assertEquals(createFunction("MIN", "TIMESTAMP").extractFinalResult(mergedTimestamps), Long.MAX_VALUE - 1);
  }

  @Test
  void nonNumericEmptyIntermediateResult() {
    for (String dataType : new String[]{"STRING", "BOOLEAN", "TIMESTAMP"}) {
      ModeAggregationFunction function = createFunction("MIN", dataType);
      // A server without matching rows sends an empty map
      Map<?, Long> empty =
          roundTrip(function, function.extractAggregationResult(function.createAggregationResultHolder()));
      assertEquals(empty, Map.of());
      assertNull(function.extractFinalResult(empty));
      assertNull(function.extractFinalResult((Map<?, Long>) null));
    }
    ModeAggregationFunction stringMode = createFunction("MIN", "STRING");
    Object2LongOpenHashMap<String> strings = new Object2LongOpenHashMap<>(Map.of("a", 1L));
    assertEquals(stringMode.merge(roundTrip(stringMode, new Int2LongOpenHashMap()), strings), strings);
    assertEquals(stringMode.merge(strings, new Int2LongOpenHashMap()), strings);
  }

  @Test
  void nonNumericRejectsUnsupportedArguments() {
    assertThrows(IllegalArgumentException.class, () -> createFunction("AVG", "STRING"));
    assertThrows(IllegalArgumentException.class, () -> createFunction("MIN", "INT"));
    assertThrows(IllegalArgumentException.class, () -> createFunction("MIN", "BYTES"));
  }

  private static ModeAggregationFunction createFunction(String reducer, String dataType) {
    return new ModeAggregationFunction(List.of(ExpressionContext.forIdentifier("myField"),
        ExpressionContext.forLiteral(DataType.STRING, reducer),
        ExpressionContext.forLiteral(DataType.STRING, dataType)), false);
  }

  private static Map<?, Long> roundTrip(ModeAggregationFunction function, Map<?, Long> intermediateResult) {
    SerializedIntermediateResult serialized = function.serializeIntermediateResult(intermediateResult);
    return function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
  }
}
