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
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.JsonResponseEncoder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests collection-valued results returned by the gRPC JDBC result set.
public class PinotGrpcResultSetTest {

  @Test
  public void testGetMapAndArrays()
      throws Exception {
    String[] columnNames = {
        "map", "booleans", "ints", "longs", "floats", "doubles", "decimals", "timestamps", "strings", "bytes",
        "uuids"
    };
    ColumnDataType[] columnTypes = {
        ColumnDataType.MAP, ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.INT_ARRAY, ColumnDataType.LONG_ARRAY,
        ColumnDataType.FLOAT_ARRAY, ColumnDataType.DOUBLE_ARRAY, ColumnDataType.BIG_DECIMAL_ARRAY,
        ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.STRING_ARRAY, ColumnDataType.BYTES_ARRAY,
        ColumnDataType.UUID_ARRAY
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
        new byte[][]{new byte[]{0, (byte) 0xff}, new byte[]{0x10, 0x20}},
        new UUID[]{UUID.fromString("00000000-0000-0000-0000-000000000001"),
            UUID.fromString("00000000-0000-0000-0000-000000000002")}
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
    Assert.assertEquals(resultSet.getObject(11), List.of(
        UUID.fromString("00000000-0000-0000-0000-000000000001"),
        UUID.fromString("00000000-0000-0000-0000-000000000002")));

    ResultSetMetaData metadata = resultSet.getMetaData();
    Assert.assertEquals(metadata.getColumnType(1), Types.JAVA_OBJECT);
    Assert.assertEquals(metadata.getColumnClassName(1), Map.class.getTypeName());
    for (int columnIndex = 2; columnIndex <= metadata.getColumnCount(); columnIndex++) {
      Assert.assertEquals(metadata.getColumnType(columnIndex), Types.JAVA_OBJECT);
      Assert.assertEquals(metadata.getColumnClassName(columnIndex), List.class.getTypeName());
    }
  }

  @Test
  public void testNullAndEmptyValues()
      throws Exception {
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"map", "ints", "strings"},
        new ColumnDataType[]{ColumnDataType.MAP, ColumnDataType.INT_ARRAY, ColumnDataType.STRING_ARRAY},
        new Object[]{Map.of(), new int[0], new String[0]});

    Assert.assertTrue(resultSet.next());
    setCurrentRowValue(resultSet, 0, null);
    setCurrentRowValue(resultSet, 1, null);
    Assert.assertNull(resultSet.getObject(1, List.class));
    Assert.assertTrue(resultSet.wasNull());
    Assert.assertNull(resultSet.getObject(1));
    Assert.assertTrue(resultSet.wasNull());
    Assert.assertNull(resultSet.getObject(2));
    Assert.assertTrue(resultSet.wasNull());
    Assert.assertEquals(resultSet.getObject(3), List.of());
    Assert.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetStringForNullScalar()
      throws Exception {
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING}, new Object[]{"placeholder"});

    Assert.assertTrue(resultSet.next());
    setCurrentRowValue(resultSet, 0, null);
    Assert.assertNull(resultSet.getString(1));
    Assert.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetUuid()
      throws Exception {
    UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"uuid"}, new ColumnDataType[]{ColumnDataType.UUID}, new Object[]{uuid});

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject(1), uuid);
    Assert.assertEquals(resultSet.getObject("uuid", UUID.class), uuid);

    ResultSetMetaData metadata = resultSet.getMetaData();
    Assert.assertEquals(metadata.getColumnType(1), Types.OTHER);
    Assert.assertEquals(metadata.getColumnClassName(1), UUID.class.getTypeName());
  }

  @Test
  public void testGetMapWithMixedNumericTypes()
      throws Exception {
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"map"},
        new ColumnDataType[]{ColumnDataType.MAP},
        new Object[]{Map.of(
            "small", 1,
            "large", 2147483648L,
            "float", 1.25d,
            "nested", Map.of("value", 2))});

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject(1), Map.of(
        "small", 1,
        "large", 2147483648L,
        "float", 1.25d,
        "nested", Map.of("value", 2)));
  }

  @Test
  public void testGetAdditionalScalarTypes()
      throws Exception {
    PinotGrpcResultSet resultSet = createResultSet(
        new String[]{"json", "decimal", "timestamp"},
        new ColumnDataType[]{ColumnDataType.JSON, ColumnDataType.BIG_DECIMAL, ColumnDataType.TIMESTAMP},
        new Object[]{"{\"key\":1}", new BigDecimal("123.450"), Timestamp.valueOf("2020-01-01 12:00:00")});

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject(1), "{\"key\":1}");
    Assert.assertEquals(resultSet.getObject(2), new BigDecimal("123.450"));
    Assert.assertEquals(resultSet.getObject(3), Timestamp.valueOf("2020-01-01 12:00:00"));
  }

  @Test
  public void testGetObjectErrors()
      throws Exception {
    DataSchema schema = new DataSchema(
        new String[]{"map", "ints", "bytes", "timestamp", "decimal"},
        new ColumnDataType[]{ColumnDataType.MAP, ColumnDataType.INT_ARRAY, ColumnDataType.BYTES,
            ColumnDataType.TIMESTAMP, ColumnDataType.BIG_DECIMAL});
    PinotGrpcResultSet resultSet = createResultSetFromFormattedRow(
        schema, new Object[]{Map.of(), new int[0], "zz", "not-a-timestamp", "not-a-decimal"});

    Assert.assertTrue(resultSet.next());
    setCurrentRowValue(resultSet, 0, "not-a-map");
    setCurrentRowValue(resultSet, 1, "not-an-array");
    Assert.expectThrows(SQLDataException.class, () -> resultSet.getObject(1));
    Assert.expectThrows(SQLDataException.class, () -> resultSet.getObject(2));
    Assert.expectThrows(SQLDataException.class, () -> resultSet.getObject(3));
    Assert.expectThrows(SQLDataException.class, () -> resultSet.getObject(4));
    Assert.expectThrows(SQLDataException.class, () -> resultSet.getObject(5));
  }

  private static void setCurrentRowValue(PinotGrpcResultSet resultSet, int columnIndex, Object value)
      throws Exception {
    Field currentRowBatchField = PinotGrpcResultSet.class.getDeclaredField("_currentRowBatch");
    currentRowBatchField.setAccessible(true);
    ResultTable currentRowBatch = (ResultTable) currentRowBatchField.get(resultSet);
    currentRowBatch.getRows().get(0)[columnIndex] = value;
  }

  private static PinotGrpcResultSet createResultSet(String[] columnNames, ColumnDataType[] columnTypes, Object[] row)
      throws Exception {
    DataSchema schema = new DataSchema(columnNames, columnTypes);
    Object[] formattedRow = row.clone();
    for (int i = 0; i < formattedRow.length; i++) {
      if (formattedRow[i] != null) {
        formattedRow[i] = columnTypes[i].format(formattedRow[i]);
      }
    }
    return createResultSetFromFormattedRow(schema, formattedRow);
  }

  private static PinotGrpcResultSet createResultSetFromFormattedRow(DataSchema schema, Object[] formattedRow)
      throws Exception {
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
