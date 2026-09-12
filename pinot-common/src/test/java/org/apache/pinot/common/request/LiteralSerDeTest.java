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
package org.apache.pinot.common.request;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies Thrift literal conversion and wire serialization.
public class LiteralSerDeTest {
  private static final short BYTES_ARRAY_FIELD_ID = 17;

  @Test
  public void testBytesArrayRoundTrip()
      throws TException {
    List<byte[][]> values = List.of(new byte[0][],
        new byte[][]{{}, {0}, {1, 2}, {(byte) 0xff}});
    List<TProtocolFactory> protocolFactories =
        List.of(new TCompactProtocol.Factory(), new TBinaryProtocol.Factory());

    for (TProtocolFactory protocolFactory : protocolFactories) {
      for (byte[][] expected : values) {
        PinotQuery query = new PinotQuery();
        query.setSelectList(List.of(RequestUtils.getLiteralExpression(expected)));

        byte[] serialized = new TSerializer(protocolFactory).serialize(query);
        PinotQuery deserialized = new PinotQuery();
        new TDeserializer(protocolFactory).deserialize(deserialized, serialized);

        Literal literal = deserialized.getSelectList().get(0).getLiteral();
        assertTrue(literal.isSetBytesArrayValue());
        assertEquals(literal.getSetField().getThriftFieldId(), BYTES_ARRAY_FIELD_ID);
        assertEquals(RequestUtils.getBytesArrayValue(literal), expected);
      }
    }
  }

  @Test
  public void testBytesArrayConversionRespectsByteBufferBounds() {
    ByteBuffer sliced = ByteBuffer.wrap(new byte[]{9, 0, 1, 2, 9});
    sliced.position(1);
    sliced.limit(4);
    ByteBuffer direct = ByteBuffer.allocateDirect(2);
    direct.put(new byte[]{3, 4});
    direct.flip();

    Literal literal = Literal.bytesArrayValue(List.of(sliced, direct));

    assertEquals(RequestUtils.getBytesArrayValue(literal), new byte[][]{{0, 1, 2}, {3, 4}});
    assertEquals(sliced.position(), 1);
    assertEquals(direct.position(), 0);
  }

  @Test
  public void testBytesArrayDeepCopy() {
    Literal literal = RequestUtils.getLiteral(new byte[][]{{0}, {1, 2}});
    Literal copy = literal.deepCopy();

    assertEquals(copy, literal);
    literal.getBytesArrayValue().get(0).put(0, (byte) 9);
    assertEquals(RequestUtils.getBytesArrayValue(copy), new byte[][]{{0}, {1, 2}});
  }

  @Test
  public void testSingleStageQueryUsesNativeBytesArrayLiteral()
      throws TException {
    for (String sql : List.of("SELECT ARRAY[X'00', X'0102'] FROM myTable",
        "SELECT id FROM myTable WHERE ARRAYS_OVERLAP(bytesMV, ARRAY[X'01'])",
        "SELECT ARRAY[X'02'], COUNT(*) FROM myTable GROUP BY ARRAY[X'02']",
        "SELECT COUNT(*) FROM myTable HAVING ARRAYS_OVERLAP(ARRAYAGG(bytesColumn, 'BYTES'), ARRAY[X'03'])",
        "SELECT id FROM myTable "
            + "ORDER BY CASE WHEN ARRAYS_OVERLAP(bytesMV, ARRAY[X'04']) THEN 1 ELSE 0 END")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery(sql);
      assertTrue(containsBytesArrayLiteral(query));

      byte[] serialized = new TSerializer(new TCompactProtocol.Factory()).serialize(query);
      PinotQuery deserialized = new PinotQuery();
      new TDeserializer(new TCompactProtocol.Factory()).deserialize(deserialized, serialized);
      assertTrue(containsBytesArrayLiteral(deserialized));
    }

    PinotQuery nested = CalciteSqlParser.compileToPinotQuery(
        "SELECT ARRAY_LENGTH(ARRAY[X'05', X'0607']) FROM myTable");
    assertEquals(nested.getSelectList().get(0).getLiteral().getIntValue(), 2);
  }

  @Test
  public void testExistingLiteralArmsStillRoundTrip()
      throws TException {
    for (Literal literal : List.of(RequestUtils.getLiteral(new byte[]{1, 2}),
        RequestUtils.getLiteral(new String[]{"a", "b"}))) {
      byte[] serialized = new TSerializer(new TCompactProtocol.Factory()).serialize(literal);
      Literal deserialized = new Literal();
      new TDeserializer(new TCompactProtocol.Factory()).deserialize(deserialized, serialized);
      assertEquals(deserialized, literal);
    }
  }

  private static boolean containsBytesArrayLiteral(Expression expression) {
    if (expression.isSetLiteral()) {
      return expression.getLiteral().isSetBytesArrayValue();
    } else if (expression.isSetFunctionCall()) {
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        if (containsBytesArrayLiteral(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsBytesArrayLiteral(PinotQuery query) {
    for (Expression expression : query.getSelectList()) {
      if (containsBytesArrayLiteral(expression)) {
        return true;
      }
    }
    if (query.isSetFilterExpression() && containsBytesArrayLiteral(query.getFilterExpression())) {
      return true;
    }
    if (query.isSetGroupByList()) {
      for (Expression expression : query.getGroupByList()) {
        if (containsBytesArrayLiteral(expression)) {
          return true;
        }
      }
    }
    if (query.isSetHavingExpression() && containsBytesArrayLiteral(query.getHavingExpression())) {
      return true;
    }
    if (query.isSetOrderByList()) {
      for (Expression expression : query.getOrderByList()) {
        if (containsBytesArrayLiteral(expression)) {
          return true;
        }
      }
    }
    return false;
  }
}
