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

package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Exercises complete V4 messages with shared scalar/MV dictionaries across transport buffer types.
public class DataTableDictionarySerDeTest {
  @DataProvider
  public Object[][] bufferKinds() {
    return new Object[][]{
        {false, false, false}, {false, false, true}, {false, true, false}, {false, true, true},
        {true, false, false}, {true, false, true}, {true, true, false}, {true, true, true}
    };
  }

  @Test(dataProvider = "bufferKinds")
  public void testCompleteMessage(boolean direct, boolean readOnly, boolean sliced)
      throws IOException {
    String[] values = {"", "first", "東京", "😀", "long-" + "x".repeat(2049), "tiny"};
    DataSchema schema = new DataSchema(new String[]{"id", "value", "values"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.STRING_ARRAY});
    DataTableBuilder builder = new DataTableBuilderV4(schema);
    for (int row = 0; row < values.length; row++) {
      builder.startRow();
      builder.setColumn(0, row);
      if (row == 0) {
        builder.setNull(1);
      } else {
        builder.setColumn(1, values[row]);
      }
      builder.setColumn(2, new String[]{values[(row + 1) % values.length], values[row]});
      builder.finishRow();
    }
    DataTable original = builder.build();
    original.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), Integer.toString(values.length));
    original.addException(QueryErrorCode.QUERY_EXECUTION, "test-消息");
    byte[] wire = original.toBytes();
    int prefix = sliced ? 13 : 0;
    ByteBuffer storage = direct
        ? ByteBuffer.allocateDirect(prefix + wire.length)
        : ByteBuffer.allocate(prefix + wire.length);
    storage.position(prefix);
    storage.put(wire).flip();
    storage.position(prefix);
    ByteBuffer input = sliced ? storage.slice() : storage;
    if (readOnly) {
      input = input.asReadOnlyBuffer();
    }

    DataTable decoded = DataTableFactory.getDataTable(input);
    assertEquals(input.position(), wire.length);
    assertEquals(decoded.getVersion(), DataTableFactory.VERSION_4);
    assertEquals(decoded.getDataSchema(), schema);
    assertEquals(decoded.getNumberOfRows(), values.length);
    assertEquals(decoded.getMetadata().get(MetadataKey.NUM_DOCS_SCANNED.getName()), Integer.toString(values.length));
    assertEquals(decoded.getExceptions(), original.getExceptions());
    assertTrue(decoded.getNullRowIds(1).contains(0));
    for (int row = 0; row < values.length; row++) {
      assertEquals(decoded.getInt(row, 0), row);
      assertEquals(decoded.getString(row, 1), row == 0 ? ColumnDataType.STRING.getNullPlaceholder() : values[row]);
      assertEquals(decoded.getStringArray(row, 2), new String[]{values[(row + 1) % values.length], values[row]});
    }
  }
}
