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
package org.apache.pinot.connector.spark.connector

import org.apache.pinot.connector.spark.connector.PinotUtils._
import org.apache.pinot.spi.data.DataSchema.ColumnDataType
import org.apache.pinot.connector.spark.BaseTest
import org.apache.pinot.connector.spark.exceptions.PinotException
import org.apache.pinot.core.common.datatable.DataTableBuilder
import org.apache.pinot.spi.data.{DataSchema, Schema}
import org.apache.pinot.spi.utils.ByteArray
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.io.Source

/**
 * Test pinot/spark conversions like schema, data table etc.
 */
class PinotUtilsTest extends BaseTest {

  test("Pinot DataTable should be converted to Spark InternalRows") {
    val columnNames = Array(
      "strCol",
      "intCol",
      "longCol",
      "floatCol",
      "doubleCol",
      "strArrayCol",
      "intArrayCol",
      "longArrayCol",
      "floatArrayCol",
      "doubleArrayCol",
      "byteType"
    )
    val columnTypes = Array(
      ColumnDataType.STRING,
      ColumnDataType.INT,
      ColumnDataType.LONG,
      ColumnDataType.FLOAT,
      ColumnDataType.DOUBLE,
      ColumnDataType.STRING_ARRAY,
      ColumnDataType.INT_ARRAY,
      ColumnDataType.LONG_ARRAY,
      ColumnDataType.FLOAT_ARRAY,
      ColumnDataType.DOUBLE_ARRAY,
      ColumnDataType.BYTES
    )
    val dataSchema = new DataSchema(columnNames, columnTypes)

    val dataTableBuilder = new DataTableBuilder(dataSchema)
    dataTableBuilder.startRow()
    dataTableBuilder.setColumn(0, "strValueDim")
    dataTableBuilder.setColumn(1, 5)
    dataTableBuilder.setColumn(2, 3L)
    dataTableBuilder.setColumn(3, 10.05f)
    dataTableBuilder.setColumn(4, 2.3d)
    dataTableBuilder.setColumn(5, Array[String]("strArr1", "null"))
    dataTableBuilder.setColumn(6, Array[Int](1, 2, 0))
    dataTableBuilder.setColumn(7, Array[Long](10L, 0))
    dataTableBuilder.setColumn(8, Array[Float](0, 15.20f))
    dataTableBuilder.setColumn(9, Array[Double](0, 10.3d))
    dataTableBuilder.setColumn(10, new ByteArray("byte_test".getBytes))
    dataTableBuilder.finishRow()
    val dataTable = dataTableBuilder.build()

    val schema = StructType(
      Seq(
        StructField("intArrayCol", ArrayType(IntegerType)),
        StructField("intCol", IntegerType),
        StructField("doubleArrayCol", ArrayType(DoubleType)),
        StructField("doubleCol", DoubleType),
        StructField("strArrayCol", ArrayType(StringType)),
        StructField("longCol", LongType),
        StructField("longArrayCol", ArrayType(LongType)),
        StructField("strCol", StringType),
        StructField("floatArrayCol", ArrayType(FloatType)),
        StructField("floatCol", FloatType),
        StructField("byteType", ArrayType(ByteType))
      )
    )

    val result = pinotDataTableToInternalRows(dataTable, schema).head
    result.getArray(0) shouldEqual ArrayData.toArrayData(Seq(1, 2, 0))
    result.getInt(1) shouldEqual 5
    result.getArray(2) shouldEqual ArrayData.toArrayData(Seq(0d, 10.3d))
    result.getDouble(3) shouldEqual 2.3d
    result.getArray(4) shouldEqual ArrayData.toArrayData(
      Seq("strArr1", "null").map(UTF8String.fromString)
    )
    result.getLong(5) shouldEqual 3L
    result.getArray(6) shouldEqual ArrayData.toArrayData(Seq(10L, 0L))
    result.getString(7) shouldEqual "strValueDim"
    result.getArray(8) shouldEqual ArrayData.toArrayData(Seq(0f, 15.20f))
    result.getFloat(9) shouldEqual 10.05f
    result.getArray(10) shouldEqual ArrayData.toArrayData("byte_test".getBytes)
  }

  test("Method should throw field not found exception while converting pinot data table") {
    val columnNames = Array("strCol", "intCol")
    val columnTypes = Array(ColumnDataType.STRING, ColumnDataType.INT)
    val dataSchema = new DataSchema(columnNames, columnTypes)

    val dataTableBuilder = new DataTableBuilder(dataSchema)
    dataTableBuilder.startRow()
    dataTableBuilder.setColumn(0, "strValueDim")
    dataTableBuilder.setColumn(1, 5)
    dataTableBuilder.finishRow()
    val dataTable = dataTableBuilder.build()

    val schema = StructType(
      Seq(
        StructField("strCol", StringType),
        StructField("intCol", IntegerType),
        StructField("longCol", LongType)
      )
    )

    val exception = intercept[PinotException] {
      pinotDataTableToInternalRows(dataTable, schema)
    }

    exception.getMessage shouldEqual s"'longCol' not found in Pinot server response"
  }

  test("Pinot schema should be converted to spark schema") {
    val pinotSchemaAsString = Source.fromResource("schema/pinot-schema.json").mkString
    val resultSchema = pinotSchemaToSparkSchema(Schema.fromString(pinotSchemaAsString))
    val sparkSchemaAsString = Source.fromResource("schema/spark-schema.json").mkString
    val sparkSchema = DataType.fromJson(sparkSchemaAsString).asInstanceOf[StructType]
    resultSchema.fields should contain theSameElementsAs sparkSchema.fields
  }

}
