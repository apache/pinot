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
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies the wire representation and compatibility boundary of Thrift literals.
public class LiteralSerDeTest {
  private static final short BYTES_ARRAY_FIELD_ID = 17;
  private static final short LEGACY_MAX_FIELD_ID = 16;

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
  public void testNativeBytesArrayArmRequiresUpgradedReader()
      throws TException {
    Literal literal = RequestUtils.getLiteral(new byte[][]{{0}, {1, 2}});
    byte[] serialized = new TSerializer(new TCompactProtocol.Factory()).serialize(literal);

    TProtocol protocol = new TCompactProtocol(new TMemoryInputTransport(serialized));
    protocol.readStructBegin();
    TField field = protocol.readFieldBegin();
    assertEquals(field.id, BYTES_ARRAY_FIELD_ID);
    assertEquals(field.type, TType.LIST);

    // This mirrors the pre-field-17 generated union reader: unknown fields are skipped and no union arm is selected.
    Short legacySetField = null;
    if (field.id <= LEGACY_MAX_FIELD_ID) {
      legacySetField = field.id;
    }
    assertFalse(field.id <= LEGACY_MAX_FIELD_ID);
    TProtocolUtil.skip(protocol, field.type);
    protocol.readFieldEnd();
    assertEquals(protocol.readFieldBegin().type, TType.STOP);
    protocol.readStructEnd();
    assertNull(legacySetField);
  }

  @Test
  public void testSingleStageQueryUsesLegacyCompatibleLiteralArms()
      throws TException {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT ARRAY['\\x00'::bytea, CAST('\\x0102' AS BYTEA)] FROM myTable "
            + "WHERE ARRAYS_OVERLAP(bytesMV, ARRAY['\\x01'::bytea])");
    assertUsesOnlyLegacyLiteralArms(query.getSelectList().get(0));
    assertUsesOnlyLegacyLiteralArms(query.getFilterExpression());

    byte[] serialized = new TSerializer(new TCompactProtocol.Factory()).serialize(query);
    PinotQuery deserialized = new PinotQuery();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(deserialized, serialized);
    assertUsesOnlyLegacyLiteralArms(deserialized.getSelectList().get(0));
    assertUsesOnlyLegacyLiteralArms(deserialized.getFilterExpression());
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

  private static void assertUsesOnlyLegacyLiteralArms(Expression expression) {
    if (expression.isSetLiteral()) {
      assertTrue(expression.getLiteral().getSetField().getThriftFieldId() <= LEGACY_MAX_FIELD_ID);
    } else if (expression.isSetFunctionCall()) {
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        assertUsesOnlyLegacyLiteralArms(operand);
      }
    }
  }
}
