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
package org.apache.pinot.query.runtime.operator;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.testutils.MockDataBlockOperatorFactory;


public class OperatorTestUtil {
  // simple key-value collision schema/data test set: "Aa" and "BB" have same hash code in java.
  private static final List<List<Object[]>> SIMPLE_KV_DATA_ROWS = Arrays.asList(
      Arrays.asList(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}),
      Arrays.asList(new Object[]{1, "AA"}, new Object[]{2, "Aa"}));
  private static final MockDataBlockOperatorFactory MOCK_OPERATOR_FACTORY;

  public static final DataSchema SIMPLE_KV_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  public static final String OP_1 = "op1";
  public static final String OP_2 = "op2";

  static {
    MOCK_OPERATOR_FACTORY = new MockDataBlockOperatorFactory()
        .registerOperator(OP_1, SIMPLE_KV_DATA_SCHEMA)
        .registerOperator(OP_2, SIMPLE_KV_DATA_SCHEMA)
        .addRows(OP_1, SIMPLE_KV_DATA_ROWS.get(0))
        .addRows(OP_2, SIMPLE_KV_DATA_ROWS.get(1));
  }

  private OperatorTestUtil() {
  }

  public static BaseOperator<TransferableBlock> getOperator(String operatorName) {
    return MOCK_OPERATOR_FACTORY.buildMockOperator(operatorName);
  }

  public static DataSchema getDataSchema(String operatorName) {
    return MOCK_OPERATOR_FACTORY.getDataSchema(operatorName);
  }

  public static TransferableBlock block(DataSchema schema, Object[]... rows) {
    return new TransferableBlock(Arrays.asList(rows), schema, DataBlock.Type.ROW);
  }
}
