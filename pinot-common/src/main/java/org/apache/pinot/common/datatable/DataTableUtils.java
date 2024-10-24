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
package org.apache.pinot.common.datatable;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.BrokerRequestIdUtils;
import org.apache.pinot.spi.config.table.TableType;

import static java.nio.charset.StandardCharsets.UTF_8;


public class DataTableUtils {
  private DataTableUtils() {
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(DataSchema dataSchema, int[] columnOffsets, int dataTableVersion) {
    assert dataTableVersion == DataTableFactory.VERSION_4;
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (storedColumnDataTypes[i]) {
        case INT:
        case FLOAT:
        case STRING:
          // For STRING, we store the dictionary id.
          rowSizeInBytes += 4;
          break;
        default:
          // This covers LONG, DOUBLE and variable length data types (POSITION|LENGTH).
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }

  /**
   * Helper method to decode string.
   */
  public static String decodeString(ByteBuffer buffer)
      throws IOException {
    int length = buffer.getInt();
    if (length == 0) {
      return StringUtils.EMPTY;
    } else {
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      return new String(bytes, UTF_8);
    }
  }

  public static TableType inferTableType(DataTable dataTable) {
    TableType tableType;
    long requestId = Long.parseLong(dataTable.getMetadata().get(DataTable.MetadataKey.REQUEST_ID.getName()));
    if (requestId == BrokerRequestIdUtils.getRealtimeRequestId(requestId)) {
      tableType = TableType.REALTIME;
    } else {
      tableType = TableType.OFFLINE;
    }
    return tableType;
  }
}
