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
package org.apache.pinot.connector.spark.common

import scala.collection.JavaConverters._

class PinotDataSourceWriteOptionsTest extends BaseTest {

  test("Spark DataSourceOptions should be converted to the PinotDataSourceWriteOptions") {
    val options = Map(
      PinotDataSourceWriteOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceWriteOptions.CONFIG_SEGMENT_NAME_FORMAT -> "segment_name",
      PinotDataSourceWriteOptions.CONFIG_PATH -> "/path/to/save",
      PinotDataSourceWriteOptions.CONFIG_INVERTED_INDEX_COLUMNS -> "col1,col2",
      PinotDataSourceWriteOptions.CONFIG_NO_DICTIONARY_COLUMNS -> "col3,col4",
      PinotDataSourceWriteOptions.CONFIG_BLOOM_FILTER_COLUMNS -> "col5,col6",
      PinotDataSourceWriteOptions.CONFIG_RANGE_INDEX_COLUMNS -> "col7,col8",
      PinotDataSourceWriteOptions.CONFIG_TIME_COLUMN_NAME -> "timeCol",
      PinotDataSourceWriteOptions.CONFIG_TIME_FORMAT -> "EPOCH|SECONDS",
      PinotDataSourceWriteOptions.CONFIG_TIME_GRANULARITY -> "1:SECONDS",
    )

    val pinotDataSourceWriteOptions = PinotDataSourceWriteOptions.from(options.asJava)

    val expected = PinotDataSourceWriteOptions(
      "tbl",
      "segment_name",
      "/path/to/save",
      "timeCol",
      "EPOCH|SECONDS",
      "1:SECONDS",
      Array("col1", "col2"),
      Array("col3", "col4"),
      Array("col5", "col6"),
      Array("col7", "col8")
    )

    pinotDataSourceWriteOptions.tableName shouldEqual expected.tableName
    pinotDataSourceWriteOptions.segmentNameFormat shouldEqual expected.segmentNameFormat
    pinotDataSourceWriteOptions.savePath shouldEqual expected.savePath
    pinotDataSourceWriteOptions.timeColumnName shouldEqual expected.timeColumnName
    pinotDataSourceWriteOptions.invertedIndexColumns shouldEqual expected.invertedIndexColumns.deep
    pinotDataSourceWriteOptions.noDictionaryColumns shouldEqual expected.noDictionaryColumns.deep
    pinotDataSourceWriteOptions.bloomFilterColumns shouldEqual expected.bloomFilterColumns.deep
    pinotDataSourceWriteOptions.rangeIndexColumns shouldEqual expected.rangeIndexColumns.deep

  }

  test("Should throw exception if `table` option is missing") {
    val options = Map(
      PinotDataSourceWriteOptions.CONFIG_PATH -> "/path/to/save"
    )

    val exception = intercept[IllegalStateException] {
      PinotDataSourceWriteOptions.from(options.asJava)
    }

    exception.getMessage shouldEqual "Table name must be specified."
  }

  test("Should throw exception if `path` option is missing") {
    val options = Map(
      PinotDataSourceWriteOptions.CONFIG_TABLE_NAME -> "tbl"
    )

    val exception = intercept[IllegalStateException] {
      PinotDataSourceWriteOptions.from(options.asJava)
    }

    exception.getMessage shouldEqual "Save path must be specified."
  }

  test("Should throw exception if `segmentNameFormat` is empty string") {
    val options = Map(
      PinotDataSourceWriteOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceWriteOptions.CONFIG_PATH -> "/path/to/save",
      PinotDataSourceWriteOptions.CONFIG_SEGMENT_NAME_FORMAT -> ""
    )

    val exception = intercept[IllegalArgumentException] {
      PinotDataSourceWriteOptions.from(options.asJava)
    }

    exception.getMessage shouldEqual "Segment name format cannot be empty string"
  }

  test("Default values should be applied for optional configurations") {
    val options = Map(
      PinotDataSourceWriteOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceWriteOptions.CONFIG_PATH -> "/path/to/save"
    )

    val pinotDataSourceWriteOptions = PinotDataSourceWriteOptions.from(options.asJava)

    val expected = PinotDataSourceWriteOptions(
      "tbl",
      "tbl-{partitionId:03}",
      "/path/to/save",
      null,
      null,
      null,
      Array.empty,
      Array.empty,
      Array.empty,
      Array.empty
    )

    pinotDataSourceWriteOptions shouldEqual expected
  }
}
