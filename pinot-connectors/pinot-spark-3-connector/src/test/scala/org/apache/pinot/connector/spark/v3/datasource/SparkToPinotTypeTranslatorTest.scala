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
      (BooleanType, FieldSpec.DataType.BOOLEAN),
      (BinaryType, FieldSpec.DataType.BYTES),
      (TimestampType, FieldSpec.DataType.LONG),
      (DateType, FieldSpec.DataType.INT)
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
      (ArrayType(BooleanType), FieldSpec.DataType.BOOLEAN),
      (ArrayType(BinaryType), FieldSpec.DataType.BYTES),
      (ArrayType(TimestampType), FieldSpec.DataType.LONG),
      (ArrayType(DateType), FieldSpec.DataType.INT)
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
}
