package org.apache.pinot.query.runtime.blocks;

import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;


public final class DataTableBlockUtils {
  private DataTableBlockUtils() {

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
