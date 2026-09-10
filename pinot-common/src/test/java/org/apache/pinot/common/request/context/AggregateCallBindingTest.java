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
package org.apache.pinot.common.request.context;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Verifies immutable logical binding and additive request serialization across supported wire protocols.
public class AggregateCallBindingTest {
  @DataProvider
  public Object[][] protocols() {
    return new Object[][]{{new TCompactProtocol.Factory()}, {new TBinaryProtocol.Factory()}};
  }

  @Test(dataProvider = "protocols")
  public void testRequestRoundTripAndLegacyFields(TProtocolFactory factory) throws Exception {
    AggregateCallBinding binding =
        new AggregateCallBinding(List.of(ColumnDataType.TIMESTAMP), ColumnDataType.TIMESTAMP);
    Function function = new Function("mode");
    function.setOperands(List.of(RequestUtils.getIdentifierExpression("eventTime")));
    function.setAggregationBinding(binding.toThrift());
    byte[] bytes = new TSerializer(factory).serialize(function);
    Function restored = new Function();
    new TDeserializer(factory).deserialize(restored, bytes);
    FunctionContext context = RequestContextUtils.getFunction(restored);
    assertEquals(context.getAggregationBinding(), binding);
    assertEquals(context.toString(), "mode(eventTime)");
    assertEquals(context.getArguments().size(), 1);
    assertEquals(RequestContextUtils.getFunction(function.deepCopy()).getAggregationBinding(), binding);

    // An older reader still sees the same operator/operands; the optional binding is a skippable field.
    TProtocol protocol = factory.getProtocol(new TMemoryInputTransport(bytes));
    protocol.readStructBegin();
    int knownFields = 0;
    for (TField field = protocol.readFieldBegin(); field.type != TType.STOP; field = protocol.readFieldBegin()) {
      if (field.id == 1) {
        assertEquals(protocol.readString(), "mode");
        knownFields++;
      } else {
        if (field.id == 2) {
          knownFields++;
        }
        TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
    assertEquals(knownFields, 2);

    function.unsetAggregationBinding();
    restored = new Function();
    new TDeserializer(factory).deserialize(restored, new TSerializer(factory).serialize(function));
    assertNull(RequestContextUtils.getFunction(restored).getAggregationBinding());
  }

  @Test
  public void testImmutableTypesAndExpressionIdentity() {
    List<ColumnDataType> types = new ArrayList<>(List.of(ColumnDataType.STRING));
    AggregateCallBinding binding = new AggregateCallBinding(types, ColumnDataType.STRING);
    types.set(0, ColumnDataType.LONG);
    assertEquals(binding.getArgumentTypes(), List.of(ColumnDataType.STRING));
    assertThrows(UnsupportedOperationException.class, () -> binding.getArgumentTypes().clear());
    List<ExpressionContext> arguments = List.of(ExpressionContext.forIdentifier("name"));
    FunctionContext unbound = new FunctionContext(FunctionContext.Type.AGGREGATION, "mode", arguments);
    FunctionContext bound = new FunctionContext(FunctionContext.Type.AGGREGATION, "mode", arguments, binding);
    assertEquals(bound, unbound);
    assertEquals(bound.hashCode(), unbound.hashCode());
    assertEquals(bound.toString(), unbound.toString());
  }

  @Test
  public void testDerivedArrayResultType() {
    ColumnDataType result = AggregationFunctionTypeResolver.inferReturnType("collect",
        ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_ARRAY),
        List.of(ExpressionContext.forIdentifier("enabled")), List.of(ColumnDataType.BOOLEAN));
    assertEquals(result, ColumnDataType.BOOLEAN_ARRAY);
  }
}
