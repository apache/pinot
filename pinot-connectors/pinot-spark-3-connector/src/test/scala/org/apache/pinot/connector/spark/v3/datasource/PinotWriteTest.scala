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

import org.apache.pinot.spi.data.Schema
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._


class PinotWriteTest extends AnyFunSuite with Matchers {
  test("Constructor should initialize writeOptions, writeSchema, and pinotSchema correctly") {
    val options = Map(
      "table" -> "testTable",
      "segmentNameFormat" -> "my_segment_format",
      "path" -> "/path/to/save",
      "timeColumnName" -> "timeCol",
      "timeFormat" -> "EPOCH|SECONDS",
      "timeGranularity" -> "1:SECONDS",
      "invertedIndexColumns" -> "col1,col2",
      "noDictionaryColumns" -> "col3,col4",
      "bloomFilterColumns" -> "col5,col6",
      "rangeIndexColumns" -> "col7,col8"
    )

    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", StringType),
      StructField("timeCol", LongType),
    ))

    val logicalWriteInfo = new TestLogicalWriteInfo(new CaseInsensitiveStringMap(options.asJava), schema)

    val pinotWrite = new PinotWrite(logicalWriteInfo)

    pinotWrite.writeOptions.tableName shouldEqual "testTable"
    pinotWrite.writeOptions.segmentNameFormat shouldEqual "my_segment_format"
    pinotWrite.writeOptions.savePath shouldEqual "/path/to/save"
    pinotWrite.writeOptions.timeColumnName shouldEqual "timeCol"
    pinotWrite.writeOptions.timeFormat shouldEqual "EPOCH|SECONDS"
    pinotWrite.writeOptions.timeGranularity shouldEqual "1:SECONDS"
    pinotWrite.writeOptions.invertedIndexColumns shouldEqual Array("col1", "col2")
    pinotWrite.writeOptions.noDictionaryColumns shouldEqual Array("col3", "col4")
    pinotWrite.writeOptions.bloomFilterColumns shouldEqual Array("col5", "col6")
    pinotWrite.writeOptions.rangeIndexColumns shouldEqual Array("col7", "col8")

    pinotWrite.writeSchema shouldEqual schema

    val expectedPinotSchema = Schema.fromString(
      """
        |{
        |  "schemaName": "testTable",
        |  "dimensionFieldSpecs": [
        |    {"name": "col1", "dataType": "STRING"},
        |    {"name": "col2", "dataType": "STRING"}
        |  ],
        |  "dateTimeFieldSpecs" : [ {
        |    "name" : "timeCol",
        |    "dataType" : "LONG",
        |    "fieldType" : "DATE_TIME",
        |    "notNull" : false,
        |    "format" : "EPOCH|SECONDS",
        |    "granularity" : "1:SECONDS"
        |  } ]
        |}
        |""".stripMargin)
    pinotWrite.pinotSchema shouldEqual expectedPinotSchema
  }
}

class TestLogicalWriteInfo(
                            options: CaseInsensitiveStringMap,
                            schema: StructType,
                            queryId: String = "testQueryId"
                          ) extends LogicalWriteInfo {
  override def options(): CaseInsensitiveStringMap = options
  override def schema(): StructType = schema
  override def queryId(): String = queryId
}
