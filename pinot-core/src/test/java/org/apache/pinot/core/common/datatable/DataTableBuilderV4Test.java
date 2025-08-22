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
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DataTableBuilderV4Test {

  @Test
  public void dataTableWithNullInStringArray()
      throws IOException {
    DataTableBuilderV4 builder = new DataTableBuilderV4(
        new DataSchema(new String[]{"col1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING_ARRAY}));
    builder.startRow();
    builder.setColumn(0, new String[]{"a", null, "b"});
    builder.finishRow();

    DataTable dataTable = builder.build();
    String[] stringArray = dataTable.getStringArray(0, 0);

    assertEquals(stringArray, new String[]{"a", null, "b"});

    dataTable.toBytes();
  }
}