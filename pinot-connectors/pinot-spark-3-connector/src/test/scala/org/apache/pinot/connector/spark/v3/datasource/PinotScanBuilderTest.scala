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

import org.apache.pinot.connector.spark.common.PinotDataSourceReadOptions
import org.apache.spark.sql.sources.{Filter, LessThan}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


class PinotScanBuilderTest extends BaseTest {
  test("Builder should build a PinotScan with given filters") {
    val optMap = new java.util.HashMap[String, String]()
    optMap.put("table", "myTable")
    optMap.put("tableType", "REALTIME")
    optMap.put("broker", "localhost:7177")
    val readOptions = PinotDataSourceReadOptions.from(optMap)

    // create a scan builder with custom schema
    val builder = new PinotScanBuilder(readOptions)
    builder.pruneColumns(StructType(
      Seq(StructField("myCol",IntegerType))
    ))

    // push a filter
    val lessThan = LessThan("myCol", 100)
    builder.pushFilters(Array[Filter]{lessThan})
    val pushedFilters = builder.pushedFilters()
    pushedFilters.length shouldEqual 1
    pushedFilters(0) shouldBe a [LessThan]

    // run the builder
    val scan = builder.build()
    scan shouldBe a [PinotScan]
  }
}
