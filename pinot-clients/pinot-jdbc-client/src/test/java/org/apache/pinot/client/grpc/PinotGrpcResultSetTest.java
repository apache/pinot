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
package org.apache.pinot.client.grpc;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.JsonResponseEncoder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies the JDBC result contract over the same metadata, schema, and data block sequence emitted by the gRPC
/// broker endpoint.
public class PinotGrpcResultSetTest {

  @Test
  public void testVariantGetStringAndGetObjectPreserveVariantNullAndSqlNull()
      throws Exception {
    PinotGrpcResultSet resultSet = new PinotGrpcResultSet(createResponses());

    assertTrue(resultSet.next());
    assertEquals(resultSet.getString(1), "{\"a\":[1,true,null],\"b\":\"text\"}");
    assertFalse(resultSet.wasNull());
    assertEquals(resultSet.getObject(1), "{\"a\":[1,true,null],\"b\":\"text\"}");
    assertFalse(resultSet.wasNull());

    assertTrue(resultSet.next());
    assertEquals(resultSet.getString(1), "null");
    assertFalse(resultSet.wasNull(), "An encoded Variant null is not SQL null");
    assertEquals(resultSet.getObject(1), "null");
    assertFalse(resultSet.wasNull(), "An encoded Variant null is not SQL null");

    assertTrue(resultSet.next());
    assertEquals(resultSet.getString(1), "\"null\"");
    assertFalse(resultSet.wasNull(), "A Variant string containing null is not SQL null");
    assertEquals(resultSet.getObject(1), "\"null\"");
    assertFalse(resultSet.wasNull(), "A Variant string containing null is not SQL null");

    assertTrue(resultSet.next());
    assertNull(resultSet.getString(1));
    assertTrue(resultSet.wasNull());
    assertNull(resultSet.getObject(1));
    assertTrue(resultSet.wasNull());
    assertFalse(resultSet.next());
  }

  private static Iterator<Broker.BrokerResponse> createResponses()
      throws IOException {
    DataSchema schema = new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});
    List<Object[]> rows = List.of(
        new Object[]{"{\"a\":[1,true,null],\"b\":\"text\"}"},
        new Object[]{"null"},
        new Object[]{"\"null\""},
        new Object[]{null}
    );
    byte[] payload = new JsonResponseEncoder().encodeResultTable(
        new ResultTable(schema, rows), 0, rows.size());

    Broker.BrokerResponse metadata = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFromUtf8("{}"))
        .build();
    Broker.BrokerResponse schemaBlock = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(schema.toBytes()))
        .build();
    Broker.BrokerResponse dataBlock = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(payload))
        .putMetadata("rowSize", Integer.toString(rows.size()))
        .putMetadata("compression", "NONE")
        .putMetadata("encoding", "JSON")
        .build();
    return List.of(metadata, schemaBlock, dataBlock).iterator();
  }
}
