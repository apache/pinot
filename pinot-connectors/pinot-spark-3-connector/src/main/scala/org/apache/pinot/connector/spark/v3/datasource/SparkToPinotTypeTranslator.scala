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

import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.pinot.spi.data.Schema.SchemaBuilder
import org.apache.spark.sql.types._


object SparkToPinotTypeTranslator {
  def translate(sparkSchema: StructType,
                tableName: String,
                timeColumn: String,
                timeFormat: String,
                timeGranularity: String): Schema = {
    val schemaBuilder = new SchemaBuilder
    schemaBuilder.setSchemaName(tableName)
    for (field <- sparkSchema.fields) {
      val fieldName = field.name
      val sparkType = field.dataType
      val pinotType = translateType(sparkType)

      (fieldName, sparkType) match {
        case (`timeColumn`, _) =>
          schemaBuilder.addDateTime(fieldName, pinotType, timeFormat, timeGranularity)
        case (_, _: ArrayType) =>
          schemaBuilder.addMultiValueDimension(fieldName, pinotType)
        case _ =>
          schemaBuilder.addSingleValueDimension(fieldName, pinotType)
      }
    }

    schemaBuilder.build
  }

  // Throws UnsupportedOperationException for types the connector cannot translate, so the
  // failure surfaces at schema-build time (driver) rather than mid-write (executor) and the
  // stack trace points at the offending type. Returning null and letting the caller rethrow
  // would scatter the error site and risk silent propagation if a future caller forgets the
  // null-check (the previous behavior).
  private def translateType(sparkType: DataType): FieldSpec.DataType = sparkType match {
    case _: StringType => FieldSpec.DataType.STRING
    case _: IntegerType => FieldSpec.DataType.INT
    case _: LongType => FieldSpec.DataType.LONG
    case _: FloatType => FieldSpec.DataType.FLOAT
    case _: DoubleType => FieldSpec.DataType.DOUBLE
    case _: DecimalType => FieldSpec.DataType.BIG_DECIMAL
    case _: BooleanType => FieldSpec.DataType.BOOLEAN
    case _: BinaryType => FieldSpec.DataType.BYTES
    case _: TimestampType => FieldSpec.DataType.LONG
    case _: DateType => FieldSpec.DataType.INT
    // Pinot does not support multi-value BYTES or multi-value BIG_DECIMAL on the segment
    // build path: ArrayType(BinaryType) would emit a multi-value BYTES dimension that the
    // segment driver rejects, and ArrayType(DecimalType) similarly has no MV BIG_DECIMAL
    // support. Reject up-front so the user sees a clear translator-time error rather than
    // a confusing segment-build failure on the executor.
    case ArrayType(_: BinaryType, _) =>
      throw new UnsupportedOperationException(
        "Multi-value BYTES (ArrayType(BinaryType)) is not supported by the Pinot writer")
    case ArrayType(_: DecimalType, _) =>
      throw new UnsupportedOperationException(
        "Multi-value BIG_DECIMAL (ArrayType(DecimalType)) is not supported by the Pinot writer")
    case ArrayType(elementType, _) => translateType(elementType)
    case other =>
      throw new UnsupportedOperationException(s"Unsupported Spark data type: $other")
  }
}
