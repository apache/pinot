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
package org.apache.pinot.connector.spark.v4.datasource

import org.apache.pinot.spi.data.FieldSpec
import org.apache.pinot.spi.data.FieldSpec.FieldType
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class SparkToPinotTypeTranslatorTest extends AnyFunSuite {

  test("Translate single value data types") {
    val typeMappings = List(
      (StringType, FieldSpec.DataType.STRING),
      (IntegerType, FieldSpec.DataType.INT),
      (LongType, FieldSpec.DataType.LONG),
      (FloatType, FieldSpec.DataType.FLOAT),
      (DoubleType, FieldSpec.DataType.DOUBLE),
      (DecimalType(38, 18), FieldSpec.DataType.BIG_DECIMAL),
      (BooleanType, FieldSpec.DataType.BOOLEAN),
      (BinaryType, FieldSpec.DataType.BYTES)
    )

    for ((sparkType, expectedPinotType) <- typeMappings) {
      val fieldName = s"${sparkType.simpleString}Field"
      val sparkSchema = StructType(Array(StructField(fieldName, sparkType)))
      val pinotSchema = SparkToPinotTypeTranslator.translate(
        sparkSchema, "table", null, null, null)
      assert(pinotSchema.getFieldSpecFor(fieldName).getDataType == expectedPinotType)
    }
  }

  test("Translate time column") {
    val sparkSchema = StructType(Array(StructField("timeField", LongType)))
    val pinotSchema = SparkToPinotTypeTranslator.translate(sparkSchema, "table", "timeField",
      "EPOCH|SECONDS", "1:SECONDS")

    val dateTimeField = pinotSchema.getDateTimeSpec("timeField")

    assert(dateTimeField != null)
    assert(dateTimeField.getFieldType == FieldType.DATE_TIME)
    assert(dateTimeField.getFormat == "EPOCH|SECONDS")
    assert(dateTimeField.getGranularity == "1:SECONDS")
  }

  test("Translate multi value data types") {
    val arrayTypeMappings = List(
      (ArrayType(StringType), FieldSpec.DataType.STRING),
      (ArrayType(IntegerType), FieldSpec.DataType.INT),
      (ArrayType(LongType), FieldSpec.DataType.LONG),
      (ArrayType(FloatType), FieldSpec.DataType.FLOAT),
      (ArrayType(DoubleType), FieldSpec.DataType.DOUBLE),
      (ArrayType(BooleanType), FieldSpec.DataType.BOOLEAN)
    )

    for ((sparkArrayType, expectedPinotType) <- arrayTypeMappings) {
      val fieldName = s"${sparkArrayType.simpleString}Field"
      val sparkSchema = StructType(Array(StructField(fieldName, sparkArrayType)))
      val pinotSchema = SparkToPinotTypeTranslator.translate(sparkSchema, "table",
        null, null, null)
      assert(pinotSchema.getFieldSpecFor(fieldName).getDataType == expectedPinotType)
      assert(!pinotSchema.getFieldSpecFor(fieldName).isSingleValueField)
    }
  }

  test("Reject ArrayType(BinaryType) and ArrayType(DecimalType) — Pinot has no MV support") {
    // Pinot rejects multi-value BYTES and multi-value BIG_DECIMAL columns on the segment
    // build path, so the translator must reject up-front rather than letting a failure
    // surface on the executor.
    val rejected = List(
      ArrayType(BinaryType),
      ArrayType(DecimalType(38, 18))
    )
    for (sparkArrayType <- rejected) {
      val sparkSchema = StructType(Array(StructField("field", sparkArrayType)))
      val ex = intercept[UnsupportedOperationException] {
        SparkToPinotTypeTranslator.translate(sparkSchema, "table", null, null, null)
      }
      assert(ex.getMessage.contains("not supported"))
    }
  }

  test("Reject TimestampType and DateType — unit semantics do not round-trip") {
    // Spark's TimestampType (microseconds-since-epoch) and DateType (days-since-epoch)
    // do not match Pinot's millis-since-epoch convention, so naively mapping to LONG/INT
    // would silently produce wrong-by-1000 timestamps. Reject so the user is forced to
    // cast to LongType (millis) or StringType upstream.
    val timestampSchema = StructType(Array(StructField("ts", TimestampType)))
    val tsEx = intercept[UnsupportedOperationException] {
      SparkToPinotTypeTranslator.translate(timestampSchema, "table", null, null, null)
    }
    assert(tsEx.getMessage.contains("TimestampType"))
    assert(tsEx.getMessage.contains("Cast to LongType"))

    val dateSchema = StructType(Array(StructField("d", DateType)))
    val dEx = intercept[UnsupportedOperationException] {
      SparkToPinotTypeTranslator.translate(dateSchema, "table", null, null, null)
    }
    assert(dEx.getMessage.contains("DateType"))
    assert(dEx.getMessage.contains("Cast to StringType") || dEx.getMessage.contains("LongType"))
  }

  test("Reject unknown Spark types with a clear error at translation time") {
    // Map/Struct/Null/CalendarInterval and similar types are not part of Pinot's data
    // model. Translator must throw immediately rather than returning null and letting
    // some downstream caller silently skip the field.
    val rejected: List[DataType] = List(
      MapType(StringType, IntegerType),
      StructType(Seq(StructField("inner", IntegerType))),
      NullType
    )
    for (sparkType <- rejected) {
      val sparkSchema = StructType(Array(StructField("field", sparkType)))
      val ex = intercept[UnsupportedOperationException] {
        SparkToPinotTypeTranslator.translate(sparkSchema, "table", null, null, null)
      }
      assert(ex.getMessage.contains("Unsupported Spark data type"))
    }
  }
}
