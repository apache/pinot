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

import java.math.BigDecimal;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.segment.local.customobject.AvgPrecisionPair;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.bytesToHex;


public class AvgPrecisionAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(FieldSpec.DataType.INT),
        new DataTypeScenario(FieldSpec.DataType.LONG),
        new DataTypeScenario(FieldSpec.DataType.FLOAT),
        new DataTypeScenario(FieldSpec.DataType.DOUBLE),
        new DataTypeScenario(FieldSpec.DataType.BIG_DECIMAL)
    };
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select avgprecision(myField) from testTable")
        .thenResultIs("STRING",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select avgprecision(myField) from testTable")
        .thenResultIs("STRING", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', avgprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | "
            + FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', avgprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "3",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select avgprecision(myField) from testTable")
        .thenResultIs("STRING", "1.6");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "5",
            "null"
        ).andOnSecondInstance("myField",
            "2",
            "null",
            "3"
        ).whenQuery("select avgprecision(myField) from testTable")
        .thenResultIs("STRING", "3.333333333333333333333333333333333");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "2",
            "null"
        ).whenQuery("select 'literal', avgprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | " + "1.666666666666666666666666666666667");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select 'literal', avgprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | " + getStringValueOfAvg(8, 2, scenario.getDataType()));
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
        .whenQuery("select tags, avgprecision(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | " + "1.5",
            "tag2    | " + "0.75",
            "tag3    | " + getStringValueOfAvg(0, 2, scenario.getDataType())
        )
        .whenQueryWithNullHandlingEnabled("select tags, avgprecision(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | " + "1.5",
            "tag2    | " + "1.5",
            "tag3    | null"
        );
  }

  @Test
  void testAvgPrecisionWithPrecisionParameter() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{10.123456789},
            new Object[]{20.987654321}
        )
        .andOnSecondInstance(
            new Object[]{30.555555555}
        )
        .whenQuery("select avgprecision(value, 10) from testTable")
        .thenResultIs("STRING", "20.55555556");
  }

  @Test
  void testAvgPrecisionWithPrecisionAndScale() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{100.123},
            new Object[]{200.456}
        )
        .andOnSecondInstance(
            new Object[]{300.789}
        )
        .whenQuery("select avgprecision(value, 10, 2) from testTable")
        .thenResultIs("STRING", "200.46");
  }

  @Test
  void testAvgPrecisionWithBigDecimalValues() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.BIG_DECIMAL, "0")
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"123456789012345678901234567890.123456789"},
            new Object[]{"987654321098765432109876543210.987654321"}
        )
        .andOnSecondInstance(
            new Object[]{"555555555555555555555555555555.555555555"}
        )
        .whenQuery("select avgprecision(value) from testTable")
        .thenResultIs("STRING", "555555555222222222188888888885.5556");
  }

  @Test
  void testAvgPrecisionWithDifferentRoundingModes() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{10.125},
            new Object[]{20.125}
        )
        .andOnSecondInstance(
            new Object[]{30.125}
        )
        .whenQuery("select avgprecision(value, 10, 2, 'UP') from testTable")
        .thenResultIs("STRING", "20.13")
        .whenQuery("select avgprecision(value, 10, 2, 'DOWN') from testTable")
        .thenResultIs("STRING", "20.12")
        .whenQuery("select avgprecision(value, 10, 2, 'CEILING') from testTable")
        .thenResultIs("STRING", "20.13")
        .whenQuery("select avgprecision(value, 10, 2, 'FLOOR') from testTable")
        .thenResultIs("STRING", "20.12")
        .whenQuery("select avgprecision(value, 10, 2, 'HALF_UP') from testTable")
        .thenResultIs("STRING", "20.13")
        .whenQuery("select avgprecision(value, 10, 2, 'HALF_DOWN') from testTable")
        .thenResultIs("STRING", "20.12")
        .whenQuery("select avgprecision(value, 10, 2, 'HALF_EVEN') from testTable")
        .thenResultIs("STRING", "20.12");
  }

  @Test
  void testAvgPrecisionWithZeroValues() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{0.0},
            new Object[]{0.0}
        )
        .andOnSecondInstance(
            new Object[]{0.0}
        )
        .whenQuery("select avgprecision(value) from testTable")
        .thenResultIs("STRING", "0.0");
  }

  @Test
  void testAvgPrecisionWithNegativeValues() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{-10.5},
            new Object[]{20.5}
        )
        .andOnSecondInstance(
            new Object[]{-5.0}
        )
        .whenQuery("select avgprecision(value, 10, 2) from testTable")
        .thenResultIs("STRING", "1.67");
  }

  @Test
  void testAvgPrecisionWithMixedPositiveNegativeAndNull() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{-100.5},
            new Object[]{null}
        )
        .andOnSecondInstance(
            new Object[]{200.5},
            new Object[]{null}
        )
        .whenQueryWithNullHandlingEnabled("select avgprecision(value, 10, 2) from testTable")
        .thenResultIs("STRING", "50.00");
  }

  @Test
  void testAvgPrecisionWithSingleValue() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{42.123456789}
        )
        .whenQuery("select avgprecision(value, 10, 4) from testTable")
        .thenResultIs("STRING", "42.1235");
  }

  @Test
  void testAvgPrecisionWithVeryLargePrecision() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.DOUBLE, 0.0)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1.0},
            new Object[]{2.0}
        )
        .andOnSecondInstance(
            new Object[]{3.0}
        )
        .whenQuery("select avgprecision(value, 100, 50) from testTable")
        .thenResultIs("STRING", "2.00000000000000000000000000000000000000000000000000");
  }

  @Test
  void testAvgPrecisionWithBytesInput() {
    // Serialize BigDecimal values to bytes and convert to hex strings for FluentQueryTest
    byte[] bytes1 = new AvgPrecisionPair(new BigDecimal("100.5"), 1).toBytes();
    byte[] bytes2 = new AvgPrecisionPair(new BigDecimal("200.5"), 1).toBytes();
    byte[] bytes3 = new AvgPrecisionPair(new BigDecimal("300.5"), 1).toBytes();

    // Convert byte arrays to hex strings (FluentQueryTest expects hex-encoded strings for BYTES type)
    String hex1 = bytesToHex(bytes1);
    String hex2 = bytesToHex(bytes2);
    String hex3 = bytesToHex(bytes3);

    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.BYTES, new byte[0])
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{hex1},
            new Object[]{hex2}
        )
        .andOnSecondInstance(
            new Object[]{hex3}
        )
        .whenQuery("select avgprecision(value, 10, 2) from testTable")
        .thenResultIs("STRING", "200.50");
  }

  @Test
  void testAvgPrecisionWithStringInput() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addSingleValueDimension("value", FieldSpec.DataType.STRING, "0")
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"123.456"},
            new Object[]{"789.012"}
        )
        .andOnSecondInstance(
            new Object[]{"345.678"}
        )
        .whenQuery("select avgprecision(value, 10, 3) from testTable")
        .thenResultIs("STRING", "419.382");
  }

  private String getStringValueOfAvg(int sum, int count, FieldSpec.DataType dataType) {
    if (dataType == FieldSpec.DataType.FLOAT || dataType == FieldSpec.DataType.DOUBLE) {
      return String.valueOf((double) sum / count);
    } else {
      // For integer types, return the exact division result
      if (sum % count == 0) {
        return String.valueOf(sum / count);
      } else {
        // Return decimal representation
        return String.valueOf((double) sum / count);
      }
    }
  }
}
