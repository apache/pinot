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
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.JsonResponseEncoder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests collection-valued results returned by the gRPC JDBC result set.
 */
public class PinotGrpcResultSetTest {

  @Test
  public void testGetMapAndArrays()
      throws Exception {
    String[] columnNames = {
        "map", "booleans", "ints", "longs", "floats", "doubles", "decimals", "timestamps", "strings", "bytes"
    };
    ColumnDataType[] columnTypes = {
        ColumnDataType.MAP, ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.INT_ARRAY, ColumnDataType.LONG_ARRAY,
        ColumnDataType.FLOAT_ARRAY, ColumnDataType.DOUBLE_ARRAY, ColumnDataType.BIG_DECIMAL_ARRAY,
        ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.STRING_ARRAY, ColumnDataType.BYTES_ARRAY
    };
    Object[] row = {
        Map.of("name", "pinot", "count", 2),
        new boolean[]{true, false},
        new int[]{1, 2},
        new long[]{2147483648L, 3L},
        new float[]{1.25f, 2.5f},
        new double[]{1.5, 2.75},
        new BigDecimal[]{new BigDecimal("1.20"), new BigDecimal("3.4")},
        new Timestamp[]{Timestamp.valueOf("2020-01-01 12:00:00"), Timestamp.valueOf("2021-02-03 04:05:06")},
        new String[]{"first", "second"},
        new byte[][]{new byte[]{0, (byte) 0xff}, new byte[]{0x10, 0x20}}
    };

    PinotGrpcResultSet resultSet = createResultSet(columnNames, columnTypes, row);

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject("map"), Map.of("name", "pinot", "count", 2));
    Assert.assertEquals(resultSet.getObject(2), List.of(true, false));
    Assert.assertEquals(resultSet.getObject(3), List.of(1, 2));
    Assert.assertEquals(resultSet.getObject(4), List.of(2147483648L, 3L));
    Assert.assertEquals(resultSet.getObject(5), List.of(1.25f, 2.5f));
    Assert.assertEquals(resultSet.getObject(6), List.of(1.5, 2.75));
    Assert.assertEquals(resultSet.getObject(7), List.of(new BigDecimal("1.20"), new BigDecimal("3.4")));
    Assert.assertEquals(resultSet.getObject(8),
        List.of(Timestamp.valueOf("2020-01-01 12:00:00"), Timestamp.valueOf("2021-02-03 04:05:06")));
    Assert.assertEquals(resultSet.getObject(9), List.of("first", "second"));
    List<?> bytes = (List<?>) resultSet.getObject(10);
    Assert.assertEquals(bytes.get(0), new byte[]{0, (byte) 0xff});
    Assert.assertEquals(bytes.get(1), new byte[]{0x10, 0x20});

    ResultSetMetaData metadata = resultSet.getMetaData();
    Assert.assertEquals(metadata.getColumnType(1), Types.JAVA_OBJECT);
    Assert.assertEquals(metadata.getColumnClassName(1), Map.class.getTypeName());
    for (int columnIndex = 2; columnIndex <= metadata.getColumnCount(); columnIndex++) {
      Assert.assertEquals(metadata.getColumnType(columnIndex), Types.JAVA_OBJECT);
      Assert.assertEquals(metadata.getColumnClassName(columnIndex), List.class.getTypeName());
    }
  }

  @Test
  public void testEmptyArrays()
      throws Exception {
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"ints", "strings"},
        new ColumnDataType[]{ColumnDataType.INT_ARRAY, ColumnDataType.STRING_ARRAY},
        new Object[]{new int[0], new String[0]});

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject(1), List.of());
    Assert.assertEquals(resultSet.getObject(2), List.of());
  }

  private static PinotGrpcResultSet createResultSet(String[] columnNames, ColumnDataType[] columnTypes, Object[] row)
      throws Exception {
    DataSchema schema = new DataSchema(columnNames, columnTypes);
    Object[] formattedRow = row.clone();
    for (int i = 0; i < formattedRow.length; i++) {
      formattedRow[i] = columnTypes[i].format(formattedRow[i]);
    }
    List<Object[]> rows = new ArrayList<>();
    rows.add(formattedRow);
    byte[] encodedRows = new JsonResponseEncoder().encodeResultTable(new ResultTable(schema, rows), 0, rows.size());

    Broker.BrokerResponse metadataResponse = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFromUtf8("{}"))
        .build();
    Broker.BrokerResponse schemaResponse = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(schema.toBytes()))
        .build();
    Broker.BrokerResponse rowsResponse = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(encodedRows))
        .putMetadata("rowSize", Integer.toString(rows.size()))
        .putMetadata(CommonConstants.Broker.Grpc.COMPRESSION, "NONE")
        .putMetadata(CommonConstants.Broker.Grpc.ENCODING, "JSON")
        .build();
    return new PinotGrpcResultSet(List.of(metadataResponse, schemaResponse, rowsResponse).iterator());
  }
}
