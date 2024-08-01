package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.FieldSpec
import org.apache.pinot.spi.data.Schema
import org.apache.pinot.spi.data.Schema.SchemaBuilder
import org.apache.spark.sql.types._


object SparkToPinotTypeTranslator {
  // TODO: incorpoate time column
  // TODO: add support for multi-value columns
  def translate(sparkSchema: StructType): Schema = {
    val schemaBuilder = new SchemaBuilder
    for (field <- sparkSchema.fields) {
      val fieldName = field.name
      val sparkType = field.dataType
      val pinotType = translateType(sparkType)
      if (pinotType != null) schemaBuilder.addSingleValueDimension(fieldName, pinotType)
      else throw new UnsupportedOperationException("Unsupported data type: " + sparkType)
    }
    schemaBuilder.build
  }

  private def translateType(sparkType: DataType) = sparkType match {
    case _: StringType => FieldSpec.DataType.STRING
    case _: IntegerType => FieldSpec.DataType.INT
    case _: LongType => FieldSpec.DataType.LONG
    case _: FloatType => FieldSpec.DataType.FLOAT
    case _: DoubleType => FieldSpec.DataType.DOUBLE
    case _: BooleanType => FieldSpec.DataType.BOOLEAN
    case _: BinaryType => FieldSpec.DataType.BYTES
    case _: TimestampType => FieldSpec.DataType.LONG
    case _: DateType => FieldSpec.DataType.INT
    case _ =>
      // TODO add full support
      null
  }
}
