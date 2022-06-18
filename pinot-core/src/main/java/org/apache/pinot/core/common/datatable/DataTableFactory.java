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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataTableFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableFactory.class);
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;
  public static final int VERSION_4 = 4;
  public static final int DEFAULT_VERSION = VERSION_3;

  private static int _version = DataTableFactory.DEFAULT_VERSION;

  private DataTableFactory() {
  }

  public static int getDataTableVersion() {
    return _version;
  }

  public static void setDataTableVersion(int version) {
    LOGGER.info("Setting DataTable version to: " + version);
    if (version != DataTableFactory.VERSION_2 && version != DataTableFactory.VERSION_3
        && version != DataTableFactory.VERSION_4) {
      throw new IllegalArgumentException("Unsupported version: " + version);
    }
    _version = version;
  }

  public static DataTable getEmptyDataTable() {
    switch (_version) {
      case VERSION_2:
        return new DataTableImplV2();
      case VERSION_3:
        return new DataTableImplV3();
      case VERSION_4:
        return new DataTableImplV4();
      default:
        throw new IllegalStateException("Unexpected value: " + _version);
    }
  }

  public static DataTableBuilder getDataTableBuilder(DataSchema dataSchema) {
    switch (_version) {
      case VERSION_2:
      case VERSION_3:
        return new DataTableBuilderV2V3(dataSchema, _version);
      case VERSION_4:
        return new DataTableBuilderV4(dataSchema);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + _version);
    }
  }

  public static DataTable getDataTable(ByteBuffer byteBuffer)
      throws IOException {
    int version = byteBuffer.getInt();
    switch (version) {
      case VERSION_2:
        return new DataTableImplV2(byteBuffer);
      case VERSION_3:
        return new DataTableImplV3(byteBuffer);
      case VERSION_4:
        return new DataTableImplV4(byteBuffer);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + version);
    }
  }

  public static DataTable getDataTable(byte[] bytes)
      throws IOException {
    return getDataTable(ByteBuffer.wrap(bytes));
  }
}
