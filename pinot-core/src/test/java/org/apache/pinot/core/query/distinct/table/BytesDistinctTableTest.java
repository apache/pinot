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
package org.apache.pinot.core.query.distinct.table;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for {@link BytesDistinctTable}.
 */
public class BytesDistinctTableTest {
  private static final String UUID_COLUMN = "uuidCol";
  private static final String UUID_VALUE_1 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_VALUE_2 = "550e8400-e29b-41d4-a716-446655440001";

  @Test
  public void testToResultTableFormatsUuidAndBytesWithoutOrderBy() {
    BytesDistinctTable uuidTable = new BytesDistinctTable(
        new DataSchema(new String[]{UUID_COLUMN}, new ColumnDataType[]{ColumnDataType.UUID}), 10, false, null);
    uuidTable.addUnbounded(new ByteArray(UuidUtils.toBytes(UUID_VALUE_1)));

    ResultTable uuidResultTable = uuidTable.toResultTable();
    assertEquals(uuidResultTable.getRows().get(0)[0], UUID_VALUE_1);

    byte[] bytesValue = new byte[]{0x01, 0x23, 0x45};
    BytesDistinctTable bytesTable = new BytesDistinctTable(
        new DataSchema(new String[]{"bytesCol"}, new ColumnDataType[]{ColumnDataType.BYTES}), 10, false, null);
    bytesTable.addUnbounded(new ByteArray(bytesValue));

    ResultTable bytesResultTable = bytesTable.toResultTable();
    assertEquals(bytesResultTable.getRows().get(0)[0], BytesUtils.toHexString(bytesValue));
  }

  @Test
  public void testToResultTableFormatsUuidWithOrderBy() {
    BytesDistinctTable uuidTable = new BytesDistinctTable(
        new DataSchema(new String[]{UUID_COLUMN}, new ColumnDataType[]{ColumnDataType.UUID}), 10, false,
        new OrderByExpressionContext(ExpressionContext.forIdentifier(UUID_COLUMN), true));
    uuidTable.addUnbounded(new ByteArray(UuidUtils.toBytes(UUID_VALUE_2)));
    uuidTable.addUnbounded(new ByteArray(UuidUtils.toBytes(UUID_VALUE_1)));

    ResultTable resultTable = uuidTable.toResultTable();
    assertEquals(resultTable.getRows().get(0)[0], UUID_VALUE_1);
    assertEquals(resultTable.getRows().get(1)[0], UUID_VALUE_2);
  }
}
