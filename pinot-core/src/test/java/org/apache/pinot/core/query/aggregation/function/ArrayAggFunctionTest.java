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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctLongFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggLongFunction;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ArrayAggFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  void aggregationAllNullsWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'INT') from testTable")
        .thenResultIs(new Object[]{new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE,
            Integer.MIN_VALUE}});
  }

  @Test
  void aggregationAllNullsWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'LONG') from testTable")
        .thenResultIs(new Object[]{new long[0]});
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select 'literal', arrayagg(myField, 'FLOAT') from testTable group by 'literal'")
        .thenResultIs(new Object[]{"literal", new float[]{Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY,
            Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}});
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select 'literal', arrayagg(myField, 'DOUBLE') from testTable group by 'literal'")
        .thenResultIs(new Object[]{"literal", new double[0]});
  }

  @Test
  void aggregationIntWithNullHandlingDisabled() {
    // Use repeated segment values because order of processing across segments isn't deterministic and not relevant
    // to this test
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "1",
            "null",
            "2"
        ).whenQuery("select arrayagg(myField, 'INT') from testTable")
        .thenResultIs(new Object[]{new int[]{1, Integer.MIN_VALUE, 2, 1, Integer.MIN_VALUE, 2}});
  }

  @Test
  void aggregationIntWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "1",
            "null",
            "2"
        ).whenQuery("select arrayagg(myField, 'INT') from testTable")
        .thenResultIs(new Object[]{new int[]{1, 2, 1, 2}});
  }

  @Test
  void aggregationDistinctIntWithNullHandlingDisabled() {
    // Use a single value in the segment because ordering is currently not deterministic due to the use of a hashset in
    // distinct array agg
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'INT', true) from testTable")
        .thenResultIs(new Object[]{new int[]{Integer.MIN_VALUE}});
  }

  @Test
  void aggregationDistinctIntWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1",
            "null",
            "1"
        ).whenQuery("select arrayagg(myField, 'INT', true) from testTable")
        .thenResultIs(new Object[]{new int[]{1}});
  }

  @Test
  void aggregationGroupBySVIntWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(false)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'INT') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Integer.MIN_VALUE, new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE}},
            new Object[]{1, new int[]{1, 1}},
            new Object[]{2, new int[]{2, 2}}
        );
  }

  @Test
  void aggregationGroupBySVIntWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(true)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'INT') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1, new int[]{1, 1}},
            new Object[]{2, new int[]{2, 2}},
            new Object[]{null, new int[0]}
        );
  }

  @Test
  void aggregationDistinctGroupBySVIntWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(false)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'INT', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Integer.MIN_VALUE, new int[]{Integer.MIN_VALUE}},
            new Object[]{1, new int[]{1}},
            new Object[]{2, new int[]{2}}
        );
  }

  @Test
  void aggregationDistinctGroupBySVIntWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.INT).getDeclaringTable(true)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'INT', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1, new int[]{1}},
            new Object[]{2, new int[]{2}},
            new Object[]{null, new int[0]}
        );
  }

  @Test
  void aggregationLongWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(false)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "1",
            "null",
            "2"
        ).whenQuery("select arrayagg(myField, 'LONG') from testTable")
        .thenResultIs(new Object[]{new long[]{1, Long.MIN_VALUE, 2, 1, Long.MIN_VALUE, 2}});
  }

  @Test
  void aggregationLongWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "1",
            "null",
            "2"
        ).whenQuery("select arrayagg(myField, 'LONG') from testTable")
        .thenResultIs(new Object[]{new long[]{1, 2, 1, 2}});
  }

  @Test
  void aggregationDistinctLongWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'LONG', true) from testTable")
        .thenResultIs(new Object[]{new long[]{Long.MIN_VALUE}});
  }

  @Test
  void aggregationDistinctLongWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1",
            "null",
            "1"
        ).whenQuery("select arrayagg(myField, 'LONG', true) from testTable")
        .thenResultIs(new Object[]{new long[]{1}});
  }

  @Test
  void aggregationGroupBySVLongWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(false)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'LONG') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Long.MIN_VALUE, new long[]{Long.MIN_VALUE, Long.MIN_VALUE}},
            new Object[]{1L, new long[]{1, 1}},
            new Object[]{2L, new long[]{2, 2}}
        );
  }

  @Test
  void aggregationGroupBySVLongWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'LONG') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1L, new long[]{1, 1}},
            new Object[]{2L, new long[]{2, 2}},
            new Object[]{null, new long[0]}
        );
  }

  @Test
  void aggregationDistinctGroupBySVLongWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(false)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'LONG', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Long.MIN_VALUE, new long[]{Long.MIN_VALUE}},
            new Object[]{1L, new long[]{1}},
            new Object[]{2L, new long[]{2}}
        );
  }

  @Test
  void aggregationDistinctGroupBySVLongWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField", "1", "2", "null")
        .andOnSecondInstance("myField", "1", "2", "null")
        .whenQuery("select myField, arrayagg(myField, 'LONG', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1L, new long[]{1}},
            new Object[]{2L, new long[]{2}},
            new Object[]{null, new long[0]}
        );
  }

  @Test
  void aggregationFloatWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).andOnSecondInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).whenQuery("select arrayagg(myField, 'FLOAT') from testTable")
        .thenResultIs(new Object[]{new float[]{1.0f, Float.NEGATIVE_INFINITY, 2.0f, 1.0f, Float.NEGATIVE_INFINITY,
            2.0f}});
  }

  @Test
  void aggregationFloatWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).andOnSecondInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).whenQuery("select arrayagg(myField, 'FLOAT') from testTable")
        .thenResultIs(new Object[]{new float[]{1.0f, 2.0f, 1.0f, 2.0f}});
  }

  @Test
  void aggregationDistinctFloatWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'FLOAT', true) from testTable")
        .thenResultIs(new Object[]{new float[]{Float.NEGATIVE_INFINITY}});
  }

  @Test
  void aggregationDistinctFloatWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "1.0"
        ).whenQuery("select arrayagg(myField, 'FLOAT', true) from testTable")
        .thenResultIs(new Object[]{new float[]{1.0f}});
  }

  @Test
  void aggregationGroupBySVFloatWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'FLOAT') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Float.NEGATIVE_INFINITY, new float[]{Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY}},
            new Object[]{1.0f, new float[]{1.0f, 1.0f}},
            new Object[]{2.0f, new float[]{2.0f, 2.0f}}
        );
  }

  @Test
  void aggregationGroupBySVFloatWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1.0")
        .andOnSecondInstance("myField", "null", "1.0")
        .whenQuery("select myField, arrayagg(myField, 'FLOAT') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1.0f, new float[]{1.0f, 1.0f}},
            new Object[]{null, new float[0]}
        );
  }

  @Test
  void aggregationDistinctGroupBySVFloatWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'FLOAT', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Float.NEGATIVE_INFINITY, new float[]{Float.NEGATIVE_INFINITY}},
            new Object[]{1.0f, new float[]{1.0f}},
            new Object[]{2.0f, new float[]{2.0f}}
        );
  }

  @Test
  void aggregationDistinctGroupBySVFloatWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.FLOAT).getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1.0")
        .andOnSecondInstance("myField", "null", "1.0")
        .whenQuery("select myField, arrayagg(myField, 'FLOAT', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1.0f, new float[]{1.0f}},
            new Object[]{null, new float[0]}
        );
  }

  @Test
  void aggregationDoubleWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(false)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).andOnSecondInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).whenQuery("select arrayagg(myField, 'DOUBLE') from testTable")
        .thenResultIs(new Object[]{new double[]{1.0, Double.NEGATIVE_INFINITY, 2.0, 1.0, Double.NEGATIVE_INFINITY,
            2.0}});
  }

  @Test
  void aggregationDoubleWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).andOnSecondInstance("myField",
            "1.0",
            "null",
            "2.0"
        ).whenQuery("select arrayagg(myField, 'DOUBLE') from testTable")
        .thenResultIs(new Object[]{new double[]{1.0, 2.0, 1.0, 2.0}});
  }

  @Test
  void aggregationDistinctDoubleWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'DOUBLE', true) from testTable")
        .thenResultIs(new Object[]{new double[]{Double.NEGATIVE_INFINITY}});
  }

  @Test
  void aggregationDistinctDoubleWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(true)
        .onFirstInstance("myField",
            "1.0",
            "null",
            "1.0"
        ).whenQuery("select arrayagg(myField, 'DOUBLE', true) from testTable")
        .thenResultIs(new Object[]{new double[]{1.0}});
  }

  @Test
  void aggregationGroupBySVDoubleWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'DOUBLE') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Double.NEGATIVE_INFINITY, new double[]{Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}},
            new Object[]{1.0, new double[]{1.0, 1.0}},
            new Object[]{2.0, new double[]{2.0, 2.0}}
        );
  }

  @Test
  void aggregationGroupBySVDoubleWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'DOUBLE') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1.0, new double[]{1.0, 1.0}},
            new Object[]{2.0, new double[]{2.0, 2.0}},
            new Object[]{null, new double[0]}
        );
  }

  @Test
  void aggregationDistinctGroupBySVDoubleWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(false)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'DOUBLE', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{Double.NEGATIVE_INFINITY, new double[]{Double.NEGATIVE_INFINITY}},
            new Object[]{1.0, new double[]{1.0}},
            new Object[]{2.0, new double[]{2.0}}
        );
  }

  @Test
  void aggregationDistinctGroupBySVDoubleWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.DOUBLE).getDeclaringTable(true)
        .onFirstInstance("myField", "null", "1.0", "2.0")
        .andOnSecondInstance("myField", "null", "1.0", "2.0")
        .whenQuery("select myField, arrayagg(myField, 'DOUBLE', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{1.0, new double[]{1.0}},
            new Object[]{2.0, new double[]{2.0}},
            new Object[]{null, new double[0]}
        );
  }

  @Test
  void aggregationStringWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(false)
        .onFirstInstance("myField",
            "a",
            "null",
            "b"
        ).andOnSecondInstance("myField",
            "a",
            "null",
            "b"
        ).whenQuery("select arrayagg(myField, 'STRING') from testTable")
        .thenResultIs(new Object[]{new String[]{"a", "null", "b", "a", "null", "b"}});
  }

  @Test
  void aggregationStringWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(true)
        .onFirstInstance("myField",
            "a",
            "null",
            "b"
        ).andOnSecondInstance("myField",
            "a",
            "null",
            "b"
        ).whenQuery("select arrayagg(myField, 'STRING') from testTable")
        .thenResultIs(new Object[]{new String[]{"a", "b", "a", "b"}});
  }

  @Test
  void aggregationDistinctStringWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).whenQuery("select arrayagg(myField, 'STRING', true) from testTable")
        .thenResultIs(new Object[]{new String[]{"null"}});
  }

  @Test
  void aggregationDistinctStringWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(true)
        .onFirstInstance("myField",
            "a",
            "null",
            "a"
        ).whenQuery("select arrayagg(myField, 'STRING', true) from testTable")
        .thenResultIs(new Object[]{new String[]{"a"}});
  }

  @Test
  void aggregationGroupBySVStringWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(false)
        .onFirstInstance("myField", "a", "b", "null")
        .andOnSecondInstance("myField", "a", "b", "null")
        .whenQuery("select myField, arrayagg(myField, 'STRING') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{"a", new String[]{"a", "a"}},
            new Object[]{"b", new String[]{"b", "b"}},
            new Object[]{"null", new String[]{"null", "null"}}
        );
  }

  @Test
  void aggregationGroupBySVStringWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(true)
        .onFirstInstance("myField", "a", "b", "null")
        .andOnSecondInstance("myField", "a", "b", "null")
        .whenQuery("select myField, arrayagg(myField, 'STRING') from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{"a", new String[]{"a", "a"}},
            new Object[]{"b", new String[]{"b", "b"}},
            new Object[]{null, new String[0]}
        );
  }

  @Test
  void aggregationDistinctGroupBySVStringWithNullHandlingDisabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(false)
        .onFirstInstance("myField", "a", "b", "null")
        .andOnSecondInstance("myField", "a", "b", "null")
        .whenQuery("select myField, arrayagg(myField, 'STRING', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{"a", new String[]{"a"}},
            new Object[]{"b", new String[]{"b"}},
            new Object[]{"null", new String[]{"null"}}
        );
  }

  @Test
  void aggregationDistinctGroupBySVStringWithNullHandlingEnabled() {
    new DataTypeScenario(FieldSpec.DataType.STRING).getDeclaringTable(true)
        .onFirstInstance("myField", "a", "b", "null")
        .andOnSecondInstance("myField", "a", "b", "null")
        .whenQuery("select myField, arrayagg(myField, 'STRING', true) from testTable group by myField order by myField")
        .thenResultIs(
            new Object[]{"a", new String[]{"a"}},
            new Object[]{"b", new String[]{"b"}},
            new Object[]{null, new String[0]}
        );
  }

  @Test
  void aggregationGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addMetricField("value", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 0},
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 0},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, arrayagg(value, 'INT') from testTable group by tags order by tags")
        .thenResultIs(new Object[]{"tag1", new int[]{0, 0}}, new Object[]{"tag2", new int[]{0, 0, 0, 0}},
            new Object[]{"tag3", new int[]{0, 0}})
        .whenQueryWithNullHandlingEnabled("select tags, arrayagg(value, 'INT') from testTable group by tags "
            + "order by tags")
        .thenResultIs(new Object[]{"tag1", new int[]{0, 0}}, new Object[]{"tag2", new int[]{0, 0}},
            new Object[]{"tag3", new int[]{}});
  }

  @Test
  void aggregationDistinctGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addMetricField("value", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 0},
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 0},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, arrayagg(value, 'INT', true) from testTable group by tags order by tags")
        .thenResultIs(new Object[]{"tag1", new int[]{0}}, new Object[]{"tag2", new int[]{0}},
            new Object[]{"tag3", new int[]{0}})
        .whenQueryWithNullHandlingEnabled("select tags, arrayagg(value, 'INT', true) from testTable group by tags "
            + "order by tags")
        .thenResultIs(new Object[]{"tag1", new int[]{0}}, new Object[]{"tag2", new int[]{0}},
            new Object[]{"tag3", new int[]{}});
  }

  @Test
  public void testDoubleArrayAggMultipleBlocks() {
    ArrayAggDistinctDoubleFunction arrayAggDistinctDoubleFunction = new ArrayAggDistinctDoubleFunction(
        ExpressionContext.forIdentifier("myField"), false);
    AggregationResultHolder aggregationResultHolder = arrayAggDistinctDoubleFunction.createAggregationResultHolder();
    arrayAggDistinctDoubleFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Double.create(null, new double[]{1.0, 2.0})));
    arrayAggDistinctDoubleFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Double.create(null, new double[]{2.0, 3.0})));
    DoubleOpenHashSet distinctResult = aggregationResultHolder.getResult();
    assertEquals(distinctResult.size(), 3);

    ArrayAggDoubleFunction arrayAggDoubleFunction = new ArrayAggDoubleFunction(
        ExpressionContext.forIdentifier("myField"), false);
    aggregationResultHolder = arrayAggDoubleFunction.createAggregationResultHolder();
    arrayAggDoubleFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Double.create(null, new double[]{1.0, 2.0})));
    arrayAggDoubleFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Double.create(null, new double[]{2.0, 3.0})));
    DoubleArrayList result = aggregationResultHolder.getResult();
    assertEquals(result.size(), 4);
  }

  @Test
  public void testLongArrayAggMultipleBlocks() {
    ArrayAggDistinctLongFunction arrayAggDistinctLongFunction = new ArrayAggDistinctLongFunction(
        ExpressionContext.forIdentifier("myField"), FieldSpec.DataType.LONG, false);
    AggregationResultHolder aggregationResultHolder = arrayAggDistinctLongFunction.createAggregationResultHolder();
    arrayAggDistinctLongFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Long.create(null, new long[]{1L, 2L})));
    arrayAggDistinctLongFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Long.create(null, new long[]{2L, 3L})));
    LongOpenHashSet distinctResult = aggregationResultHolder.getResult();
    assertEquals(distinctResult.size(), 3);

    ArrayAggLongFunction arrayAggLongFunction = new ArrayAggLongFunction(
        ExpressionContext.forIdentifier("myField"), FieldSpec.DataType.LONG, false);
    aggregationResultHolder = arrayAggLongFunction.createAggregationResultHolder();
    arrayAggLongFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Long.create(null, new long[]{1L, 2L})));
    arrayAggLongFunction.aggregate(2, aggregationResultHolder,
        Map.of(ExpressionContext.forIdentifier("myField"),
            SyntheticBlockValSets.Long.create(null, new long[]{1L, 2L})));
    LongArrayList result = aggregationResultHolder.getResult();
    assertEquals(result.size(), 4);
  }
}
