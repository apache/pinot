/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common.datatable;

import com.linkedin.pinot.common.utils.DataTable;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DataTableFactory {
  private DataTableFactory() {
  }

  public static DataTable getDataTable(ByteBuffer byteBuffer) throws IOException {
    int version = byteBuffer.getInt();
    switch (version) {
      case 2:
        return new DataTableImplV2(byteBuffer);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + version);
    }
  }

  public static DataTable getDataTable(byte[] bytes) throws IOException {
    return getDataTable(ByteBuffer.wrap(bytes));
  }
}
