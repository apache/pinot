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
package org.apache.pinot.connector.spark.datasource

import org.apache.pinot.connector.spark.{BaseTest, datasource}
import org.apache.pinot.connector.spark.exceptions.PinotException
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.JavaConverters._

/**
 * Test datasource read configs and defaults values.
 */
class PinotDataSourceReadOptionsTest extends BaseTest {

  test("Spark DataSourceOptions should be converted to the PinotDataSourceReadOptions") {
    val options = Map(
      DataSourceOptions.TABLE_KEY -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "hybrid",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_SEGMENTS_PER_SPLIT -> "1",
      PinotDataSourceReadOptions.CONFIG_USE_PUSH_DOWN_FILTERS -> "false",
      PinotDataSourceReadOptions.CONFIG_USE_GRPC_SERVER -> "false",
    )

    val datasourceOptions = new DataSourceOptions(options.asJava)
    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(datasourceOptions)

    val expected =
      PinotDataSourceReadOptions(
        "tbl",
        None,
        "localhost:9000",
        "localhost:8000",
        false,
        1,
        10000,
        false
      )

    pinotDataSourceReadOptions shouldEqual expected
  }

  test("Method should throw exception if `tableType` option is missing or wrong") {
    // missing
    val missingOption = Map(
      DataSourceOptions.TABLE_KEY -> "tbl",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    // wrong input
    val wrongOption = Map(
      DataSourceOptions.TABLE_KEY -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offlinee",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val missingException = intercept[PinotException] {
      PinotDataSourceReadOptions.from(new DataSourceOptions(missingOption.asJava))
    }

    val wrongException = intercept[PinotException] {
      PinotDataSourceReadOptions.from(new DataSourceOptions(wrongOption.asJava))
    }

    missingException.getMessage shouldEqual "`tableType` should be specified"
    wrongException.getMessage shouldEqual "Unknown `tableType`: OFFLINEE"
  }

}
