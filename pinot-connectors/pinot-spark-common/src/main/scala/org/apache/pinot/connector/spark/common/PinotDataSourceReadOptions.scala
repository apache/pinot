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
  val CONFIG_USE_HTTPS = "useHttps"
  // Unified security switch: when set, it implies HTTPS for HTTP and TLS for gRPC (unless overridden)
  val CONFIG_SECURE_MODE = "secureMode"
  val CONFIG_KEYSTORE_PATH = "keystorePath"
  val CONFIG_KEYSTORE_PASSWORD = "keystorePassword"
  val CONFIG_TRUSTSTORE_PATH = "truststorePath"
  val CONFIG_TRUSTSTORE_PASSWORD = "truststorePassword"
  val CONFIG_AUTH_HEADER = "authHeader"
  val CONFIG_AUTH_TOKEN = "authToken"
  
  // Proxy configuration
  val CONFIG_PROXY_ENABLED = "proxy.enabled"
  
  // gRPC configuration
  val CONFIG_GRPC_PORT = "grpc.port"
  val CONFIG_GRPC_MAX_INBOUND_MESSAGE_SIZE = "grpc.max-inbound-message-size"
  val CONFIG_GRPC_USE_PLAIN_TEXT = "grpc.use-plain-text"
  val CONFIG_GRPC_TLS_KEYSTORE_TYPE = "grpc.tls.keystore-type"
  val CONFIG_GRPC_TLS_KEYSTORE_PATH = "grpc.tls.keystore-path"
  val CONFIG_GRPC_TLS_KEYSTORE_PASSWORD = "grpc.tls.keystore-password"
  val CONFIG_GRPC_TLS_TRUSTSTORE_TYPE = "grpc.tls.truststore-type"
  val CONFIG_GRPC_TLS_TRUSTSTORE_PATH = "grpc.tls.truststore-path"
  val CONFIG_GRPC_TLS_TRUSTSTORE_PASSWORD = "grpc.tls.truststore-password"
  val CONFIG_GRPC_TLS_SSL_PROVIDER = "grpc.tls.ssl-provider"
  val CONFIG_GRPC_PROXY_URI = "grpc.proxy-uri"
  
  val QUERY_OPTIONS_DELIMITER = ","
  private[pinot] val DEFAULT_CONTROLLER: String = "localhost:9000"
  private[pinot] val DEFAULT_USE_PUSH_DOWN_FILTERS: Boolean = true
  private[pinot] val DEFAULT_SEGMENTS_PER_SPLIT: Int = 3
  private[pinot] val DEFAULT_PINOT_SERVER_TIMEOUT_MS: Long = 10000
  private[pinot] val DEFAULT_USE_GRPC_SERVER: Boolean = false
  private[pinot] val DEFAULT_FAIL_ON_INVALID_SEGMENTS = false
  private[pinot] val DEFAULT_USE_HTTPS: Boolean = false
  private[pinot] val DEFAULT_SECURE_MODE: Boolean = false
  private[pinot] val DEFAULT_PROXY_ENABLED: Boolean = false
  private[pinot] val DEFAULT_GRPC_PORT: Int = 8090
  private[pinot] val DEFAULT_GRPC_MAX_INBOUND_MESSAGE_SIZE: Long = 128 * 1024 * 1024 // 128MB
  private[pinot] val DEFAULT_GRPC_USE_PLAIN_TEXT: Boolean = true
  private[pinot] val DEFAULT_GRPC_TLS_KEYSTORE_TYPE: String = "JKS"
  private[pinot] val DEFAULT_GRPC_TLS_TRUSTSTORE_TYPE: String = "JKS"
  private[pinot] val DEFAULT_GRPC_TLS_SSL_PROVIDER: String = "JDK"

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
    // Unified security mode: if provided, it controls defaults for both HTTPS and gRPC TLS
    val secureModeDefined = options.containsKey(CONFIG_SECURE_MODE)
    val secureModeValue = if (secureModeDefined) options.getBoolean(CONFIG_SECURE_MODE, DEFAULT_SECURE_MODE) else DEFAULT_SECURE_MODE
    // Parse HTTPS configuration early so it can be used for broker discovery. Precedence: explicit useHttps, else secureMode, else default
    val useHttps = if (options.containsKey(CONFIG_USE_HTTPS)) options.getBoolean(CONFIG_USE_HTTPS, DEFAULT_USE_HTTPS) else secureModeValue
    val keystorePath = Option(options.get(CONFIG_KEYSTORE_PATH)).filter(_.nonEmpty)
    val keystorePassword = Option(options.get(CONFIG_KEYSTORE_PASSWORD)).filter(_.nonEmpty)
    val truststorePath = Option(options.get(CONFIG_TRUSTSTORE_PATH)).filter(_.nonEmpty)
    val truststorePassword = Option(options.get(CONFIG_TRUSTSTORE_PASSWORD)).filter(_.nonEmpty)
    val authHeader = Option(options.get(CONFIG_AUTH_HEADER)).filter(_.nonEmpty)
    val authToken = Option(options.get(CONFIG_AUTH_TOKEN)).filter(_.nonEmpty)
    
    // Parse proxy configuration
    val proxyEnabled = options.getBoolean(CONFIG_PROXY_ENABLED, DEFAULT_PROXY_ENABLED)
    
    // Parse gRPC configuration
    val grpcPort = options.getInt(CONFIG_GRPC_PORT, DEFAULT_GRPC_PORT)
    val grpcMaxInboundMessageSize = options.getLong(CONFIG_GRPC_MAX_INBOUND_MESSAGE_SIZE, DEFAULT_GRPC_MAX_INBOUND_MESSAGE_SIZE)
    // gRPC plain-text: explicit flag wins; otherwise, if secureMode is true, we want TLS (plain-text = false)
    // If secureMode is false, default to plaintext (true)
    val grpcUsePlainText = if (options.containsKey(CONFIG_GRPC_USE_PLAIN_TEXT)) {
      options.getBoolean(CONFIG_GRPC_USE_PLAIN_TEXT, DEFAULT_GRPC_USE_PLAIN_TEXT)
    } else {
      if (secureModeValue) false else true
    }
    val grpcTlsKeystoreType = options.getOrDefault(CONFIG_GRPC_TLS_KEYSTORE_TYPE, DEFAULT_GRPC_TLS_KEYSTORE_TYPE)
    val grpcTlsKeystorePath = Option(options.get(CONFIG_GRPC_TLS_KEYSTORE_PATH)).filter(_.nonEmpty)
    val grpcTlsKeystorePassword = Option(options.get(CONFIG_GRPC_TLS_KEYSTORE_PASSWORD)).filter(_.nonEmpty)
    val grpcTlsTruststoreType = options.getOrDefault(CONFIG_GRPC_TLS_TRUSTSTORE_TYPE, DEFAULT_GRPC_TLS_TRUSTSTORE_TYPE)
    val grpcTlsTruststorePath = Option(options.get(CONFIG_GRPC_TLS_TRUSTSTORE_PATH)).filter(_.nonEmpty)
    val grpcTlsTruststorePassword = Option(options.get(CONFIG_GRPC_TLS_TRUSTSTORE_PASSWORD)).filter(_.nonEmpty)
    val grpcTlsSslProvider = options.getOrDefault(CONFIG_GRPC_TLS_SSL_PROVIDER, DEFAULT_GRPC_TLS_SSL_PROVIDER)
    val grpcProxyUri = Option(options.get(CONFIG_GRPC_PROXY_URI)).filter(_.nonEmpty)

    // Configure HTTPS client if needed
    if (useHttps) {
      HttpUtils.configureHttpsClient(keystorePath, keystorePassword, truststorePath, truststorePassword)
    }

    val broker = options.get(PinotDataSourceReadOptions.CONFIG_BROKER) match {
      case s if s == null || s.isEmpty =>
        val brokerInstances = PinotClusterClient.getBrokerInstances(controller, tableName, useHttps, authHeader, authToken, proxyEnabled)
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
      useHttps,
      keystorePath,
      keystorePassword,
      truststorePath,
      truststorePassword,
      authHeader,
      authToken,
      proxyEnabled,
      grpcPort,
      grpcMaxInboundMessageSize,
      grpcUsePlainText,
      grpcTlsKeystoreType,
      grpcTlsKeystorePath,
      grpcTlsKeystorePassword,
      grpcTlsTruststoreType,
      grpcTlsTruststorePath,
      grpcTlsTruststorePassword,
      grpcTlsSslProvider,
      grpcProxyUri
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
    useHttps: Boolean,
    keystorePath: Option[String],
    keystorePassword: Option[String],
    truststorePath: Option[String],
    truststorePassword: Option[String],
    authHeader: Option[String],
    authToken: Option[String],
    proxyEnabled: Boolean,
    grpcPort: Int,
    grpcMaxInboundMessageSize: Long,
    grpcUsePlainText: Boolean,
    grpcTlsKeystoreType: String,
    grpcTlsKeystorePath: Option[String],
    grpcTlsKeystorePassword: Option[String],
    grpcTlsTruststoreType: String,
    grpcTlsTruststorePath: Option[String],
    grpcTlsTruststorePassword: Option[String],
    grpcTlsSslProvider: String,
    grpcProxyUri: Option[String])
