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

import com.fasterxml.jackson.core.`type`.TypeReference
import java.net.{URI, URLEncoder}
import org.apache.pinot.connector.spark.common.query.ScanQuery
import org.apache.pinot.spi.config.table.TableType
import org.apache.pinot.spi.data.Schema
import org.apache.pinot.spi.utils.builder.TableNameBuilder

import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * PinotCusterClient reads metadata from Pinot controller.
 */
private[pinot] object PinotClusterClient extends Logging {
  private val TABLE_SCHEMA_TEMPLATE = "%s://%s/tables/%s/schema"
  private val TABLE_BROKER_INSTANCES_TEMPLATE = "%s://%s/tables/livebrokers?tables=%s"
  private val TIME_BOUNDARY_TEMPLATE = "%s://%s/debug/timeBoundary/%s"
  private val ROUTING_TABLE_TEMPLATE = "%s://%s/debug/routingTable/sql?query=%s"
  private val ROUTING_TABLE_SIMPLE_TEMPLATE = "%s://%s/debug/routingTable/%s"
  private val INSTANCES_API_TEMPLATE = "%s://%s/instances/%s"

  def getTableSchema(controllerUrl: String, tableName: String, useHttps: Boolean = false, authHeader: Option[String] = None, authToken: Option[String] = None, proxyEnabled: Boolean = false): Schema = {
    val rawTableName = TableNameBuilder.extractRawTableName(tableName)
    Try {
      val scheme = if (useHttps) "https" else "http"
      // When proxy is enabled, always go through the proxy base (controllerUrl is expected to be the proxy host)
      val targetUrl = controllerUrl
      val uri = new URI(String.format(TABLE_SCHEMA_TEMPLATE, scheme, targetUrl, rawTableName))
      val response = HttpUtils.sendGetRequest(uri, authHeader, authToken)
      Schema.fromString(response)
    } match {
      case Success(response) =>
        logDebug(s"Pinot schema received successfully for table '$rawTableName'")
        response
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while getting Pinot schema for table '$rawTableName'",
          exception
        )
    }
  }

  /**
   * Get available broker urls(host:port) for given table.
   * This method is used when if broker instances not defined in the datasource options.
   */
  def getBrokerInstances(controllerUrl: String, tableName: String, useHttps: Boolean = false, authHeader: Option[String] = None, authToken: Option[String] = None, proxyEnabled: Boolean = false): List[String] = {
    Try {
      val scheme = if (useHttps) "https" else "http"
      val targetUrl = controllerUrl
      val uri = new URI(String.format(TABLE_BROKER_INSTANCES_TEMPLATE, scheme, targetUrl, tableName))
      val response = HttpUtils.sendGetRequest(uri, authHeader, authToken)

      // Decode the JSON response into a map of table name to list of LiveBrokerInfo objects
      val typeRef = new TypeReference[Map[String, List[BrokerInstanceInfo]]] {}
      val tableToInstancesMap = decodeTo(response, typeRef)

      // Extract the LiveBrokerInfo list for the requested table
      val brokerInfos = tableToInstancesMap.getOrElse(tableName, List.empty[BrokerInstanceInfo])

      // Map the broker info objects to "host:port" strings
      val brokerUrls = brokerInfos.map { brokerInfo =>
        s"${brokerInfo.host}:${brokerInfo.port}"
      }

      if (brokerUrls.isEmpty) {
        throw new IllegalStateException(s"Not found broker instance for table '$tableName'")
      }

      brokerUrls
    } match {
      case Success(result) =>
        logDebug(s"Broker instances received successfully for table '$tableName'")
        result
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while getting broker instances for table '$tableName'",
          exception
        )
    }
  }

  /**
   * Get time boundary info of specified table.
   * This method is used when table is hybrid to ensure that the overlap
   * between realtime and offline segment data is queried exactly once.
   *
   * @return time boundary info if table exist and segments push type is 'append' or None otherwise
   */
  def getTimeBoundaryInfo(brokerUrl: String, tableName: String, useHttps: Boolean = false, authHeader: Option[String] = None, authToken: Option[String] = None, proxyEnabled: Boolean = false): Option[TimeBoundaryInfo] = {
    val rawTableName = TableNameBuilder.extractRawTableName(tableName)
    Try {
      // pinot converts the given table name to the offline table name automatically
      val scheme = if (useHttps) "https" else "http"
      val targetUrl = brokerUrl
      val uri = new URI(String.format(TIME_BOUNDARY_TEMPLATE, scheme, targetUrl, rawTableName))
      val response = HttpUtils.sendGetRequest(uri, authHeader, authToken)
      decodeTo(response, classOf[TimeBoundaryInfo])
    } match {
      case Success(decodedResponse) =>
        logDebug(s"Received time boundary for table $tableName, $decodedResponse")
        Some(decodedResponse)
      case Failure(exception) =>
        exception match {
          case e: HttpStatusCodeException if e.isStatusCodeNotFound =>
            // DO NOT THROW EXCEPTION
            // because, in hybrid table, segment push type of offline table can be 'refresh'
            // therefore, time boundary info can't found for given table
            // also if table name is incorrect, time boundary info can't found too,
            // but this method will not be called if table does not exist in pinot
            logWarning(s"Time boundary not found for table, $tableName")
            None
          case e: Exception =>
            throw PinotException(
              s"An error occurred while getting time boundary info for table '$rawTableName'",
              e
            )
        }
    }
  }

  /**
   * Fetch routing table(s) for given query(s).
   * If given table name already have type suffix, routing table found directly for given table suffix.
   * If not, offline and realtime routing tables will be got.
   *
   * Example output:
   *    - realtime ->
   *          - realtimeServer1 -> (segment1, segment2, segment3)
   *          - realtimeServer2 -> (segment4)
   *    - offline ->
   *          - offlineServer10 -> (segment10, segment20)
   *
   * @return realtime and/or offline routing table(s)
   */
  def getRoutingTable(brokerUrl: String,
                      scanQuery: ScanQuery,
                      useHttps: Boolean = false,
                      authHeader: Option[String] = None,
                      authToken: Option[String] = None,
                      proxyEnabled: Boolean = false): Map[TableType, Map[String, List[String]]] = {
    val routingTables =
      if (scanQuery.isTableOffline) {
        val offlineRoutingTable =
          getRoutingTableForQuery(brokerUrl, scanQuery.offlineSelectQuery, useHttps, authHeader, authToken, proxyEnabled)
        Map(TableType.OFFLINE -> offlineRoutingTable)
      } else if (scanQuery.isTableRealtime) {
        val realtimeRoutingTable =
          getRoutingTableForQuery(brokerUrl, scanQuery.realtimeSelectQuery, useHttps, authHeader, authToken, proxyEnabled)
        Map(TableType.REALTIME -> realtimeRoutingTable)
      } else {
        // hybrid table
        val offlineRoutingTable =
          getRoutingTableForQuery(brokerUrl, scanQuery.offlineSelectQuery, useHttps, authHeader, authToken, proxyEnabled)
        val realtimeRoutingTable =
          getRoutingTableForQuery(brokerUrl, scanQuery.realtimeSelectQuery, useHttps, authHeader, authToken, proxyEnabled)
        Map(
          TableType.OFFLINE -> offlineRoutingTable,
          TableType.REALTIME -> realtimeRoutingTable
        )
      }

    if (routingTables.values.forall(_.isEmpty)) {
      throw PinotException(s"Received routing tables are empty")
    }

    routingTables
  }

  /**
   * Get host information for a Pinot instance
   *
   * @return InstanceInfo
   */
  def getInstanceInfo(controllerUrl: String, instance: String, useHttps: Boolean = false, authHeader: Option[String] = None, authToken: Option[String] = None, proxyEnabled: Boolean = false): InstanceInfo = {
    Try {
      val scheme = if (useHttps) "https" else "http"
      val targetUrl = controllerUrl
      val uri = new URI(String.format(INSTANCES_API_TEMPLATE, scheme, targetUrl, instance))
      val response = if (proxyEnabled) {
        HttpUtils.sendGetRequestWithProxyHeaders(uri, authHeader, authToken, "controller", controllerUrl)
      } else {
        HttpUtils.sendGetRequest(uri, authHeader, authToken)
      }

      // Use the updated decodeTo function with Jackson
      decodeTo(response, classOf[InstanceInfo])
    } match {
      case Success(decodedResponse) =>
        decodedResponse
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while reading instance info for: '$instance'",
          exception
        )
    }
  }

  private def getRoutingTableForQuery(brokerUrl: String, sql: String, useHttps: Boolean = false, authHeader: Option[String] = None, authToken: Option[String] = None, proxyEnabled: Boolean = false): Map[String, List[String]] = {
    Try {
      val encodedSqlQueryParam = URLEncoder.encode(sql, "UTF-8")
      val scheme = if (useHttps) "https" else "http"
      val targetUrl = brokerUrl
      val uri = new URI(String.format(ROUTING_TABLE_TEMPLATE, scheme, targetUrl, encodedSqlQueryParam))
      val response = HttpUtils.sendGetRequest(uri, authHeader, authToken)

      decodeTo(response, classOf[Map[String, List[String]]])
    } match {
      case Success(decodedResponse) =>
        logDebug(s"Received routing table for query $sql, $decodedResponse")
        decodedResponse
      case Failure(exception) =>
        exception match {
          case e: HttpStatusCodeException if e.isStatusCodeNotFound =>
            // Fallback for older brokers without SQL-based routing debug endpoint
            val scheme = if (useHttps) "https" else "http"
            val targetUrl = brokerUrl
            // Best-effort extract table name after FROM
            val tableFromSql =
              sql.split("(?i)\\s+from\\s+").lift(1).map(_.trim.split("\\s+")(0)).getOrElse("")
            if (tableFromSql.isEmpty) {
              throw PinotException(s"Failed to parse table name from SQL for routing fallback: '$sql'", e)
            }
            val fallbackUri = new URI(String.format(ROUTING_TABLE_SIMPLE_TEMPLATE, scheme, targetUrl, tableFromSql))
            val fallbackResponse = HttpUtils.sendGetRequest(fallbackUri, authHeader, authToken)
            val decoded = decodeTo(fallbackResponse, classOf[Map[String, List[String]]])
            if (decoded.isEmpty) {
              throw PinotException(s"Received empty routing table for '$tableFromSql'")
            }
            decoded
          case _ =>
            throw PinotException(
              s"An error occurred while getting routing table for query, '$sql'",
              exception
            )
        }
    }
  }
}

private[pinot] case class TimeBoundaryInfo(timeColumn: String, timeValue: String) {

  def getOfflinePredicate: String = s""""$timeColumn" < $timeValue"""

  def getRealtimePredicate: String = s""""$timeColumn" >= $timeValue"""
}

@JsonIgnoreProperties(ignoreUnknown = true)
private[pinot] case class BrokerInstanceInfo(host: String,
                                             port: Int,
                                             instanceName: String)

@JsonIgnoreProperties(ignoreUnknown = true)
private[pinot] case class InstanceInfo(instanceName: String,
                                       hostName: String,
                                       port: String,
                                       grpcPort: Int)
