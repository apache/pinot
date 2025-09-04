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

import java.util

import org.apache.pinot.spi.config.table.TableType

import scala.util.Random

/**
 * To create serializable datasource reader options from spark datasource options.
 */
object PinotDataSourceReadOptions {
  val CONFIG_TABLE_NAME = "table"
  val CONFIG_TABLE_TYPE = "tableType"
  val CONFIG_CONTROLLER = "controller"
  val CONFIG_BROKER = "broker"
  val CONFIG_USE_PUSH_DOWN_FILTERS = "usePushDownFilters"
  val CONFIG_SEGMENTS_PER_SPLIT = "segmentsPerSplit"
  val CONFIG_PINOT_SERVER_TIMEOUT_MS = "pinotServerTimeoutMs"
  var CONFIG_USE_GRPC_SERVER = "useGrpcServer"
  val CONFIG_QUERY_OPTIONS = "queryOptions"
  val CONFIG_FAIL_ON_INVALID_SEGMENTS = "failOnInvalidSegments"
  val CONFIG_API_TOKEN = "authorization"
  val QUERY_OPTIONS_DELIMITER = ","
  private[pinot] val DEFAULT_CONTROLLER: String = "localhost:9000"
  private[pinot] val DEFAULT_USE_PUSH_DOWN_FILTERS: Boolean = true
  private[pinot] val DEFAULT_SEGMENTS_PER_SPLIT: Int = 3
  private[pinot] val DEFAULT_PINOT_SERVER_TIMEOUT_MS: Long = 10000
  private[pinot] val DEFAULT_USE_GRPC_SERVER: Boolean = false
  private[pinot] val DEFAULT_FAIL_ON_INVALID_SEGMENTS = false

  private[pinot] val tableTypes = Seq("OFFLINE", "REALTIME", "HYBRID")

  private[pinot] def from(optionsMap: util.Map[String, String]): PinotDataSourceReadOptions = {
    this.from(new CaseInsensitiveStringMap(optionsMap))
  }

  private[pinot] def from(options: CaseInsensitiveStringMap): PinotDataSourceReadOptions = {
    if (!options.containsKey(CONFIG_TABLE_NAME)) {
      throw new IllegalStateException(
        "Table name must be specified. eg: tbl_OFFLINE, tbl_REALTIME or tbl"
      )
    }
    if (!options.containsKey(CONFIG_TABLE_TYPE)) {
      throw PinotException("`tableType` should be specified")
    }

    val tableName = options.get(CONFIG_TABLE_NAME)
    val tableTypeInput = options.get(CONFIG_TABLE_TYPE).toUpperCase()
    val tableType = tableTypeInput match {
      case "REALTIME" => Some(TableType.REALTIME)
      case "OFFLINE" => Some(TableType.OFFLINE)
      case "HYBRID" => None
      case _ =>
        throw PinotException(s"Unknown `tableType`: $tableTypeInput")
    }

    // pinot cluster options
    val controller = options.getOrDefault(CONFIG_CONTROLLER, DEFAULT_CONTROLLER)
    val authorization = options.getOrDefault(CONFIG_API_TOKEN, "")
    val broker = options.get(PinotDataSourceReadOptions.CONFIG_BROKER) match {
      case s if s == null || s.isEmpty =>
        val brokerInstances = PinotClusterClient.getBrokerInstances(controller, tableName, authorization)
        Random.shuffle(brokerInstances).head
      case s => s
    }
    // connector related options
    val usePushDownFilters =
      options.getBoolean(CONFIG_USE_PUSH_DOWN_FILTERS, DEFAULT_USE_PUSH_DOWN_FILTERS)
    val segmentsPerSplit = options.getInt(CONFIG_SEGMENTS_PER_SPLIT, DEFAULT_SEGMENTS_PER_SPLIT)
    val pinotServerTimeoutMs =
      options.getLong(CONFIG_PINOT_SERVER_TIMEOUT_MS, DEFAULT_PINOT_SERVER_TIMEOUT_MS)
    val useGrpcServer = options.getBoolean(CONFIG_USE_GRPC_SERVER, DEFAULT_USE_GRPC_SERVER)
    val queryOptions = options.getOrDefault(CONFIG_QUERY_OPTIONS, "")
      .split(QUERY_OPTIONS_DELIMITER).filter(_.nonEmpty).toSet
    val failOnInvalidSegments = options.getBoolean(CONFIG_FAIL_ON_INVALID_SEGMENTS,
      DEFAULT_FAIL_ON_INVALID_SEGMENTS)

    PinotDataSourceReadOptions(
      tableName,
      tableType,
      controller,
      broker,
      usePushDownFilters,
      segmentsPerSplit,
      pinotServerTimeoutMs,
      useGrpcServer,
      queryOptions,
      failOnInvalidSegments,
      authorization,
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
    pinotServerTimeoutMs: Long,
    useGrpcServer: Boolean,
    queryOptions: Set[String],
    failOnInvalidSegments: Boolean,
    authorization: String)
