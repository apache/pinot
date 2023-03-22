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
package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.common.datatable.DataTable
import org.apache.pinot.common.utils.DataSchema.ColumnDataType
import org.apache.pinot.connector.spark.common.PinotException
import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * Helper methods for spark-pinot conversions
 */
private[pinot] object TypeConverter {

  /** Convert a Pinot schema to Spark schema. */
  def pinotSchemaToSparkSchema(schema: Schema): StructType = {
    val structFields = schema.getAllFieldSpecs.asScala.map { field =>
      val sparkDataType = pinotDataTypeToSparkDataType(field.getDataType)
      if (field.isSingleValueField) {
        StructField(field.getName, sparkDataType)
      } else {
        StructField(field.getName, ArrayType(sparkDataType))
      }
    }
    StructType(structFields.toList)
  }

  private def pinotDataTypeToSparkDataType(dataType: FieldSpec.DataType): DataType =
    dataType match {
      case FieldSpec.DataType.INT => IntegerType
      case FieldSpec.DataType.LONG => LongType
      case FieldSpec.DataType.FLOAT => FloatType
      case FieldSpec.DataType.DOUBLE => DoubleType
      case FieldSpec.DataType.STRING => StringType
      case FieldSpec.DataType.BYTES => ArrayType(ByteType)
      case FieldSpec.DataType.TIMESTAMP => LongType
      case FieldSpec.DataType.BOOLEAN => BooleanType
      case _ =>
        throw PinotException(s"Unsupported pinot data type '$dataType")
    }

  /** Convert Pinot DataTable to Seq of InternalRow */
  def pinotDataTableToInternalRows(
      dataTable: DataTable,
      sparkSchema: StructType): Seq[InternalRow] = {
    val dataTableColumnNames = dataTable.getDataSchema.getColumnNames
    val nullRowIdsByColumn = (0 until dataTable.getDataSchema.size()).map{ col =>
      dataTable.getNullRowIds(col)
    }
    (0 until dataTable.getNumberOfRows).map { rowIndex =>
      // spark schema is used to ensure columns order
      val columns = sparkSchema.fields.map { field =>
        val colIndex = dataTableColumnNames.indexOf(field.name)
        if (colIndex < 0) {
          throw PinotException(s"'${field.name}' not found in Pinot server response")
        } else {
          if (nullRowIdsByColumn(colIndex) != null
              && nullRowIdsByColumn(colIndex).contains(rowIndex)) {
            null
          } else {
            val columnDataType = dataTable.getDataSchema.getColumnDataType(colIndex)
            readPinotColumnData(dataTable, columnDataType, rowIndex, colIndex)
          }
        }
      }
      InternalRow.fromSeq(columns)
    }
  }

  private def readPinotColumnData(
     dataTable: DataTable,
     columnDataType: ColumnDataType,
     rowIndex: Int,
     colIndex: Int): Any = columnDataType match {
    // single column types
    case ColumnDataType.STRING =>
      UTF8String.fromString(dataTable.getString(rowIndex, colIndex))
    case ColumnDataType.INT =>
      dataTable.getInt(rowIndex, colIndex)
    case ColumnDataType.LONG =>
      dataTable.getLong(rowIndex, colIndex)
    case ColumnDataType.FLOAT =>
      dataTable.getFloat(rowIndex, colIndex)
    case ColumnDataType.DOUBLE =>
      dataTable.getDouble(rowIndex, colIndex)
    case ColumnDataType.TIMESTAMP =>
      dataTable.getLong(rowIndex, colIndex)
    case ColumnDataType.BOOLEAN =>
      dataTable.getInt(rowIndex, colIndex) == 1

    // array column types
    case ColumnDataType.STRING_ARRAY =>
      ArrayData.toArrayData(
        dataTable.getStringArray(rowIndex, colIndex).map(UTF8String.fromString).toSeq
      )
    case ColumnDataType.INT_ARRAY =>
      ArrayData.toArrayData(dataTable.getIntArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.LONG_ARRAY =>
      ArrayData.toArrayData(dataTable.getLongArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.FLOAT_ARRAY =>
      ArrayData.toArrayData(dataTable.getFloatArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.DOUBLE_ARRAY =>
      ArrayData.toArrayData(dataTable.getDoubleArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.BYTES =>
      ArrayData.toArrayData(dataTable.getBytes(rowIndex, colIndex).getBytes)
    case ColumnDataType.TIMESTAMP_ARRAY =>
      ArrayData.toArrayData(dataTable.getLongArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.BOOLEAN_ARRAY =>
      ArrayData.toArrayData(
        dataTable.getIntArray(rowIndex, colIndex).map(i => i == 1).toSeq
      )

    case _ =>
      throw PinotException(s"'$columnDataType' is not supported")
  }
}
