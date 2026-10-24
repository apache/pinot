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
import java.util.List;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.JsonResponseEncoder;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies the Java client result contract for data blocks emitted by the gRPC broker endpoint.
public class GrpcResultSetTest {

  @Test
  public void testEstablishedTypeSqlNullRetainsStringContract()
      throws IOException {
    DataSchema schema = new DataSchema(new String[]{"message"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    List<Object[]> rows = List.<Object[]>of(new Object[]{null});
    byte[] payload = new JsonResponseEncoder().encodeResultTable(
        new ResultTable(schema, rows), 0, rows.size());
    Broker.BrokerResponse response = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(payload))
        .putMetadata("rowSize", Integer.toString(rows.size()))
        .putMetadata("compression", "NONE")
        .putMetadata("encoding", "JSON")
        .build();

    GrpcResultSet resultSet = new GrpcResultSet(schema, response);
    assertEquals(resultSet.getString(0, 0), "null",
        "Existing column types must retain the legacy getString() representation for SQL null");
  }

  @Test
  public void testVariantGetStringPreservesVariantNullAndSqlNull()
      throws IOException {
    DataSchema schema = new DataSchema(new String[]{"payload"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.VARIANT});
    List<Object[]> rows = List.of(
        new Object[]{"{\"answer\":42}"},
        new Object[]{"null"},
        new Object[]{"\"null\""},
        new Object[]{null}
    );
    byte[] payload = new JsonResponseEncoder().encodeResultTable(
        new ResultTable(schema, rows), 0, rows.size());
    Broker.BrokerResponse response = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(payload))
        .putMetadata("rowSize", Integer.toString(rows.size()))
        .putMetadata("compression", "NONE")
        .putMetadata("encoding", "JSON")
        .build();

    GrpcResultSet resultSet = new GrpcResultSet(schema, response);
    assertEquals(resultSet.getString(0, 0), "{\"answer\":42}");
    assertEquals(resultSet.getString(1, 0), "null", "A Variant null is not SQL null");
    assertEquals(resultSet.getString(2, 0), "\"null\"", "A Variant string containing null is not SQL null");
    assertNull(resultSet.getString(3, 0), "SQL null must map to Java null");
    assertTrue(resultSet.toString().contains("null"), "Text rendering must tolerate SQL null values");
  }
}
