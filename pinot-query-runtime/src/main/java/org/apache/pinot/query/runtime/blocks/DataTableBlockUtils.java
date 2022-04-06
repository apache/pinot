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
package org.apache.pinot.query.runtime.blocks;

import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;


public final class DataTableBlockUtils {
  private DataTableBlockUtils() {
    // do not instantiate.
  }

  // used to indicate a datatable block status
  private static final DataSchema EMPTY_SCHEMA = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
  private static final DataTable EMPTY_DATATABLE = new DataTableBuilder(EMPTY_SCHEMA).build();
  private static final DataTableBlock END_OF_STREAM_DATATABLE_BLOCK = new DataTableBlock(EMPTY_DATATABLE);

  public static DataTableBlock getEndOfStreamDataTableBlock() {
    return END_OF_STREAM_DATATABLE_BLOCK;
  }

  public static DataTable getEndOfStreamDataTable() {
    return EMPTY_DATATABLE;
  }

  public static DataTable getErrorDataTable(Exception e) {
    DataTable errorDataTable = new DataTableBuilder(EMPTY_SCHEMA).build();
    errorDataTable.addException(QueryException.UNKNOWN_ERROR_CODE, e.getMessage());
    return errorDataTable;
  }

  public static DataTableBlock getErrorDatatableBlock(Exception e) {
    return new DataTableBlock(getErrorDataTable(e));
  }

  public static DataTable getEmptyDataTable(DataSchema dataSchema) {
    if (dataSchema != null) {
      return new DataTableBuilder(dataSchema).build();
    } else {
      return EMPTY_DATATABLE;
    }
  }

  public static DataTableBlock getEmptyDataTableBlock(DataSchema dataSchema) {
    return new DataTableBlock(getEmptyDataTable(dataSchema));
  }

  public static boolean isEndOfStream(DataTableBlock dataTableBlock) {
    DataSchema dataSchema = dataTableBlock.getDataTable().getDataSchema();
    return dataSchema.getColumnNames().length == 0 && dataSchema.getColumnDataTypes().length == 0;
  }
}
