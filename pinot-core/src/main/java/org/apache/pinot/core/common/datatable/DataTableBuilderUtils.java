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

import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV2;
import org.apache.pinot.common.datatable.DataTableImplV3;
import org.apache.pinot.common.datatable.DataTableImplV4;


/**
 * The <code>DataTableUtils</code> class provides utility methods for data table.
 */
@SuppressWarnings("rawtypes")
public class DataTableBuilderUtils {
  private DataTableBuilderUtils() {
  }

  /**
   * Returns an empty data table without data.
   */
  public static DataTable getEmptyDataTable() {
    int version = DataTableBuilderFactory.getDataTableVersion();
    switch (version) {
      case DataTableFactory.VERSION_2:
        return new DataTableImplV2();
      case DataTableFactory.VERSION_3:
        return new DataTableImplV3();
      case DataTableFactory.VERSION_4:
        return new DataTableImplV4();
      default:
        throw new IllegalStateException("Unsupported data table version: " + version);
    }
  }

  public static DataTable buildEmptyDataTable() {
    return getEmptyDataTable();
  }
}
