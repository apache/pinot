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

import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;


public class BooleanAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  void aggregationAllNullsWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select bool_or(myField) from testTable")
        .thenResultIs("BOOLEAN", "false");
  }

  @Test
  void aggregationAllNullsWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select bool_and(myField) from testTable")
        .thenResultIs("BOOLEAN", "null");
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', bool_and(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | false");
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', bool_or(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | null");
  }

  @Test
  void andAggregationWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "true"
        ).whenQuery("select bool_and(myField) from testTable")
        .thenResultIs("BOOLEAN", "false");
  }

  @Test
  void andAggregationWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select bool_and(myField) from testTable")
        .thenResultIs("BOOLEAN", "true");
  }

  @Test
  void andAggregationGroupBySVWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "true"
        ).whenQuery("select 'literal', bool_and(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | false");
  }

  @Test
  void andAggregationGroupBySVWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select 'literal', bool_and(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | true");
  }

  @Test
  void orAggregationWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "false"
        ).andOnSecondInstance("myField",
            "null",
            "false"
        ).whenQuery("select bool_or(myField) from testTable")
        .thenResultIs("BOOLEAN", "false");
  }

  @Test
  void orAggregationWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select bool_or(myField) from testTable")
        .thenResultIs("BOOLEAN", "true");
  }

  @Test
  void orAggregationGroupBySVWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "true"
        ).andOnSecondInstance("myField",
            "null",
            "true"
        ).whenQuery("select 'literal', bool_or(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | true");
  }

  @Test
  void orAggregationGroupBySVWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.BOOLEAN).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "false"
        ).andOnSecondInstance("myField",
            "null",
            "false"
        ).whenQuery("select 'literal', bool_or(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | BOOLEAN", "literal | false");
  }
}
