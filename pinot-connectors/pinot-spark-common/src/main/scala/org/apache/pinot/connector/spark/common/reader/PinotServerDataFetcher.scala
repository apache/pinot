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
package org.apache.pinot.connector.spark.common.reader

import org.apache.commons.lang3.tuple.Pair
import org.apache.helix.model.InstanceConfig
import org.apache.pinot.common.datatable.DataTable
import org.apache.pinot.common.metrics.BrokerMetrics
import org.apache.pinot.common.request.BrokerRequest
import org.apache.pinot.connector.spark.common.partition.PinotSplit
import org.apache.pinot.connector.spark.common.{Logging, PinotDataSourceReadOptions, PinotException}
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager
import org.apache.pinot.core.transport.{AsyncQueryResponse, QueryRouter, ServerInstance}
import org.apache.pinot.spi.config.table.TableType
import org.apache.pinot.spi.env.PinotConfiguration
import org.apache.pinot.spi.metrics.PinotMetricUtils
import org.apache.pinot.sql.parsers.CalciteSqlCompiler

import java.util.{Collections, List => JList, Map => JMap}
import scala.collection.JavaConverters._

/**
 * Actual data fetcher from Pinot server with specific segments.
 * Eg: offline-server1: segment1, segment2, segment3
 */
private[reader] class PinotServerDataFetcher(
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends Logging {
  private val brokerId = "apache_spark"
  private val metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry
  private val brokerMetrics = new BrokerMetrics(metricsRegistry)
  private val pinotConfig = new PinotConfiguration()
  private val serverRoutingStatsManager = new ServerRoutingStatsManager(pinotConfig, brokerMetrics)
  private val queryRouter = new QueryRouter(brokerId, brokerMetrics, serverRoutingStatsManager)
  // TODO add support for TLS-secured server

  def fetchData(): List[DataTable] = {
    val routingTableForRequest = createRoutingTableForRequest()

    val requestStartTime = System.nanoTime()
    val pinotServerAsyncQueryResponse = pinotSplit.serverAndSegments.serverType match {
      case TableType.REALTIME =>
        val realtimeBrokerRequest =
          CalciteSqlCompiler.compileToBrokerRequest(pinotSplit.query.realtimeSelectQuery)
        submitRequestToPinotServer(null, null, realtimeBrokerRequest, routingTableForRequest)
      case TableType.OFFLINE =>
        val offlineBrokerRequest =
          CalciteSqlCompiler.compileToBrokerRequest(pinotSplit.query.offlineSelectQuery)
        submitRequestToPinotServer(offlineBrokerRequest, routingTableForRequest, null, null)
    }

    val pinotServerResponse = pinotServerAsyncQueryResponse.getFinalResponses.values().asScala.toList
    logInfo(s"Pinot server total response time in millis: ${System.nanoTime() - requestStartTime}")

    closePinotServerConnection()

    pinotServerResponse.foreach { response =>
      logInfo(
        s"Request stats; " +
          s"responseSize: ${response.getResponseSize}, " +
          s"responseDelayMs: ${response.getResponseDelayMs}, " +
          s"deserializationTimeMs: ${response.getDeserializationTimeMs}, " +
          s"submitDelayMs: ${response.getSubmitDelayMs}"
      )
    }

    val dataTables = pinotServerResponse
      .map(_.getDataTable)
      .filter(_ != null)

    if (dataTables.isEmpty) {
      throw PinotException(s"${pinotSplit.serverAndSegments.toString} could not respond the query")
    }

    dataTables.filter(_.getNumberOfRows > 0)
  }

  private def createRoutingTableForRequest(): JMap[ServerInstance, Pair[JList[String], JList[String]]] = {
    val nullZkId: String = null
    val instanceConfig = new InstanceConfig(nullZkId)
    instanceConfig.setHostName(pinotSplit.serverAndSegments.serverHost)
    instanceConfig.setPort(pinotSplit.serverAndSegments.serverPort)
    // TODO: support netty-sec
    val serverInstance = new ServerInstance(instanceConfig)
    Map(
      serverInstance -> Pair.of(pinotSplit.serverAndSegments.segments.asJava, List[String]().asJava)
    ).asJava
  }

  private def submitRequestToPinotServer(
      offlineBrokerRequest: BrokerRequest,
      offlineRoutingTable: JMap[ServerInstance, Pair[JList[String], JList[String]]],
      realtimeBrokerRequest: BrokerRequest,
      realtimeRoutingTable: JMap[ServerInstance, Pair[JList[String], JList[String]]]): AsyncQueryResponse = {
    logInfo(s"Sending request to ${pinotSplit.serverAndSegments.toString}")
    queryRouter.submitQuery(
      partitionId,
      pinotSplit.query.rawTableName,
      offlineBrokerRequest,
      offlineRoutingTable,
      realtimeBrokerRequest,
      realtimeRoutingTable,
      dataSourceOptions.pinotServerTimeoutMs
    )
  }

  private def closePinotServerConnection(): Unit = {
    queryRouter.shutDown()
    logInfo("Pinot server connection closed")
  }
}

object PinotServerDataFetcher {

  def apply(
      partitionId: Int,
      pinotSplit: PinotSplit,
      dataSourceOptions: PinotDataSourceReadOptions): PinotServerDataFetcher = {
    new PinotServerDataFetcher(partitionId, pinotSplit, dataSourceOptions)
  }
}
