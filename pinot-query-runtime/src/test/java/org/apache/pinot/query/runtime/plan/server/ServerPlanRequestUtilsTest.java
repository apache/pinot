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
package org.apache.pinot.query.runtime.plan.server;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link ServerPlanRequestUtils}.
 */
public class ServerPlanRequestUtilsTest {
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  /**
   * Regression test for the UUID IN-predicate literal fix.
   *
   * Before the fix, UUID ByteArray values were passed as raw byte[] literals into the dynamic filter.
   * The server-side predicate evaluator expected a canonical UUID string, so the filter never matched
   * and UUID join queries silently returned no rows.
   *
   * After the fix, UUID values are emitted as canonical lowercase string literals.
   */
  @Test
  public void testComputeInOperandsUuidEmitsStringLiterals()
      throws Exception {
    DataSchema schema = new DataSchema(new String[]{"uuidCol"}, new ColumnDataType[]{ColumnDataType.UUID});

    List<Object[]> dataContainer = new ArrayList<>();
    dataContainer.add(new Object[]{new ByteArray(UuidUtils.toBytes(UUID_VALUE))});

    List<Expression> expressions = invokeComputeInOperands(dataContainer, schema, 0);

    assertEquals(expressions.size(), 1);
    Expression expr = expressions.get(0);
    assertNotNull(expr.getLiteral(), "UUID operand must be a literal expression");
    // Must be a string literal containing the canonical UUID, not a byte array literal
    assertTrue(expr.getLiteral().isSetStringValue(),
        "UUID literal must be a string, not bytes. Got: " + expr.getLiteral());
    assertEquals(expr.getLiteral().getStringValue(), UUID_VALUE,
        "UUID literal value must be canonical lowercase RFC 4122 string");
  }

  /**
   * Verifies that raw BYTES columns still emit byte-array literals (unchanged behavior).
   */
  @Test
  public void testComputeInOperandsBytesEmitsByteArrayLiterals()
      throws Exception {
    DataSchema schema = new DataSchema(new String[]{"bytesCol"}, new ColumnDataType[]{ColumnDataType.BYTES});
    byte[] rawBytes = {0x01, 0x02, 0x03};

    List<Object[]> dataContainer = new ArrayList<>();
    dataContainer.add(new Object[]{new ByteArray(rawBytes)});

    List<Expression> expressions = invokeComputeInOperands(dataContainer, schema, 0);

    assertEquals(expressions.size(), 1);
    Expression expr = expressions.get(0);
    assertNotNull(expr.getLiteral(), "BYTES operand must be a literal expression");
    assertTrue(expr.getLiteral().isSetBinaryValue(),
        "BYTES literal must be binary, not string. Got: " + expr.getLiteral());
  }

  @SuppressWarnings("unchecked")
  private static List<Expression> invokeComputeInOperands(List<Object[]> dataContainer, DataSchema dataSchema,
      int colIdx)
      throws Exception {
    Method method = ServerPlanRequestUtils.class.getDeclaredMethod("computeInOperands", List.class, DataSchema.class,
        int.class);
    method.setAccessible(true);
    return (List<Expression>) method.invoke(null, dataContainer, dataSchema, colIdx);
  }
}
