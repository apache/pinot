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

import org.apache.pinot.connector.spark.connector.PinotClusterClient
import org.apache.pinot.connector.spark.exceptions.PinotException
import org.apache.pinot.connector.spark.scalafyOptional
import org.apache.pinot.spi.config.table.TableType
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.util.Random

/**
 * To create serializable datasource reader options from spark datasource options.
 */
object PinotDataSourceReadOptions {
  val CONFIG_TABLE_TYPE = "tableType"
  val CONFIG_CONTROLLER = "controller"
  val CONFIG_BROKER = "broker"
  val CONFIG_USE_PUSH_DOWN_FILTERS = "usePushDownFilters"
  val CONFIG_SEGMENTS_PER_SPLIT = "segmentsPerSplit"
  val CONFIG_PINOT_SERVER_TIMEOUT_MS = "pinotServerTimeoutMs"
  private[pinot] val DEFAULT_CONTROLLER: String = "localhost:9000"
  private[pinot] val DEFAULT_USE_PUSH_DOWN_FILTERS: Boolean = true
  private[pinot] val DEFAULT_SEGMENTS_PER_SPLIT: Int = 3
  private[pinot] val DEFAULT_PINOT_SERVER_TIMEOUT_MS: Long = 10000

  private[pinot] val tableTypes = Seq("OFFLINE", "REALTIME", "HYBRID")

  private[pinot] def from(options: DataSourceOptions): PinotDataSourceReadOptions = {
    // tableName option
    if (!options.tableName().isPresent) {
      throw new IllegalStateException(
        "Table name must be specified. eg: tbl_OFFLINE, tbl_REALTIME or tbl"
      )
    }
    val tableName = options.tableName().get()
    // tableType option
    val tableType = scalafyOptional(options.get(CONFIG_TABLE_TYPE))
      .map(_.toUpperCase)
      .map { inputTableType =>
        if (tableTypes.contains(inputTableType)) {
          if (inputTableType == "HYBRID") None
          else Some(TableType.valueOf(inputTableType))
        } else {
          throw PinotException(s"Unknown `tableType`: $inputTableType")
        }
      }
      .getOrElse { throw PinotException("`tableType` should be specified") }
    // pinot cluster options
    val controller = scalafyOptional(options.get(CONFIG_CONTROLLER)).getOrElse(DEFAULT_CONTROLLER)
    val broker = scalafyOptional(options.get(PinotDataSourceReadOptions.CONFIG_BROKER)).getOrElse {
      val brokerInstances = PinotClusterClient.getBrokerInstances(controller, tableName)
      Random.shuffle(brokerInstances).head
    }
    // connector related options
    val usePushDownFilters =
      options.getBoolean(CONFIG_USE_PUSH_DOWN_FILTERS, DEFAULT_USE_PUSH_DOWN_FILTERS)
    val segmentsPerSplit = options.getInt(CONFIG_SEGMENTS_PER_SPLIT, DEFAULT_SEGMENTS_PER_SPLIT)
    val pinotServerTimeoutMs =
      options.getLong(CONFIG_PINOT_SERVER_TIMEOUT_MS, DEFAULT_PINOT_SERVER_TIMEOUT_MS)

    PinotDataSourceReadOptions(
      tableName,
      tableType,
      controller,
      broker,
      usePushDownFilters,
      segmentsPerSplit,
      pinotServerTimeoutMs
    )
  }
}

// tableType None if table is hybrid
private[pinot] case class PinotDataSourceReadOptions(
    tableName: String,
    tableType: Option[TableType],
    controller: String,
    broker: String,
    usePushDownFilters: Boolean,
    segmentsPerSplit: Int,
    pinotServerTimeoutMs: Long)
