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
package org.apache.pinot.core.operator.streaming;

import java.util.Arrays;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests that [StreamingResponseUtils] puts a paged data table into the gRPC payload with the same bytes as
/// `DataTable.toBytes()`.
public class StreamingResponseUtilsTest {
  private static final int NUM_ROWS = 200;
  private static final int VALUE_SIZE = 10_000;

  @Test
  public void testPayloadOfPagedDataTable()
      throws Exception {
    DataSchema dataSchema = new DataSchema(new String[]{"bytes"}, new ColumnDataType[]{ColumnDataType.BYTES});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, new ByteArray(valueOf(rowId)));
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();

    for (Server.ServerResponse response : new Server.ServerResponse[]{
        StreamingResponseUtils.getDataResponse(dataTable), StreamingResponseUtils.getNonStreamingResponse(dataTable)
    }) {
      DataTable receivedDataTable = DataTableFactory.getDataTable(response.getPayload().toByteArray());
      assertEquals(receivedDataTable.getNumberOfRows(), NUM_ROWS);
      for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
        assertEquals(receivedDataTable.getBytes(rowId, 0).getBytes(), valueOf(rowId));
      }
      // The payload must also survive the protobuf wire encoding, which writes each part of the rope.
      Server.ServerResponse parsed = Server.ServerResponse.parseFrom(response.toByteArray());
      assertEquals(parsed.getPayload(), response.getPayload());
    }
  }

  private static byte[] valueOf(int rowId) {
    byte[] value = new byte[VALUE_SIZE];
    Arrays.fill(value, (byte) rowId);
    return value;
  }
}
