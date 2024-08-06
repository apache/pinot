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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DataTableFactory {
  private DataTableFactory() {
  }

  public static final int VERSION_4 = 4;

  public static DataTable getDataTable(ByteBuffer byteBuffer)
      throws IOException {
    int version = byteBuffer.getInt();
    Preconditions.checkState(version == VERSION_4, "Unsupported data table version: %s", version);
    return new DataTableImplV4(byteBuffer);
  }

  public static DataTable getDataTable(byte[] bytes)
      throws IOException {
    return getDataTable(ByteBuffer.wrap(bytes));
  }
}
