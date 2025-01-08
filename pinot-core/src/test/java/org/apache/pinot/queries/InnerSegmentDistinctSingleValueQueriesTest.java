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
package org.apache.pinot.queries;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.query.DictionaryBasedDistinctOperator;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class InnerSegmentDistinctSingleValueQueriesTest extends BaseSingleValueQueriesTest {

  @Test
  public void testSingleColumnDistinct() {
    String query = "SELECT DISTINCT column1 FROM testTable LIMIT 1000000";
    DictionaryBasedDistinctOperator distinctOperator = getOperator(query);

    DistinctTable distinctTable = distinctOperator.nextBlock().getDistinctTable();
    assertNotNull(distinctTable);
    assertEquals(distinctTable.size(), 6582);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column1"});
    assertEquals(dataSchema.getColumnDataTypes(), new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    for (Object[] values : distinctTable.getRows()) {
      assertNotNull(values);
      assertEquals(values.length, 1);
    }
  }

  @Test
  public void testMultiColumnDistinct() {
    String query = "SELECT DISTINCT column1, column3 FROM testTable LIMIT 1000000";
    DistinctOperator distinctOperator = getOperator(query);

    DistinctTable distinctTable = distinctOperator.nextBlock().getDistinctTable();
    assertNotNull(distinctTable);
    assertEquals(distinctTable.size(), 21968);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column1", "column3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    for (Object[] values : distinctTable.getRows()) {
      assertNotNull(values);
      assertEquals(values.length, 2);
    }
  }
}
