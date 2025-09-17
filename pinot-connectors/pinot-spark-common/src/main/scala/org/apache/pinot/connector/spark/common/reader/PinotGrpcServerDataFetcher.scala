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

import io.grpc.{ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils
import org.apache.pinot.common.datatable.{DataTable, DataTableFactory}
import org.apache.pinot.common.proto.PinotQueryServerGrpc
import org.apache.pinot.common.proto.Server.ServerRequest
import org.apache.pinot.connector.spark.common.{AuthUtils, Logging, PinotDataSourceReadOptions}
import org.apache.pinot.connector.spark.common.partition.PinotSplit
import org.apache.pinot.spi.config.table.TableType

import java.io.Closeable
import java.net.URI
import scala.collection.JavaConverters._

/**
 * Data fetcher from Pinot Grpc server for specific segments.
 * Eg: offline-server1: segment1, segment2, segment3
 */
private[reader] class PinotGrpcServerDataFetcher(pinotSplit: PinotSplit, readOptions: PinotDataSourceReadOptions)
  extends Logging with Closeable {

  private val (channelHost, channelPort) = {
    if (readOptions.proxyEnabled && readOptions.grpcProxyUri.nonEmpty) {
      val proxyUri = readOptions.grpcProxyUri.get
      val (hostStr, portStr) = org.apache.pinot.connector.spark.common.NetUtils.parseHostPort(proxyUri, readOptions.useHttps)
      (hostStr, portStr.toInt)
    } else {
      (pinotSplit.serverAndSegments.serverHost, pinotSplit.serverAndSegments.serverGrpcPort)
    }
  }

  // Debug: Print out channel host and port
  logDebug(s"PinotGrpcServerDataFetcher connecting to host: $channelHost, port: $channelPort")
  private val baseChannelBuilder = ManagedChannelBuilder
    .forAddress(channelHost, channelPort)
    .maxInboundMessageSize(Math.min(readOptions.grpcMaxInboundMessageSize, Int.MaxValue.toLong).toInt)
    .asInstanceOf[ManagedChannelBuilder[_]]

  private val channel = {
    val builder =
      if (readOptions.grpcUsePlainText) baseChannelBuilder.usePlaintext()
      else baseChannelBuilder
    builder.build()
  }

  private val pinotServerBlockingStub = {
    val baseStub = PinotQueryServerGrpc.newBlockingStub(channel)

    // Attach proxy forwarding headers when proxy is enabled
    val withProxyHeaders =
      if (readOptions.proxyEnabled && readOptions.grpcProxyUri.nonEmpty) {
        val md = new Metadata()
        val forwardHostKey = Metadata.Key.of("forward_host", Metadata.ASCII_STRING_MARSHALLER)
        val forwardPortKey = Metadata.Key.of("forward_port", Metadata.ASCII_STRING_MARSHALLER)
        md.put(forwardHostKey, pinotSplit.serverAndSegments.serverHost)
        md.put(forwardPortKey, pinotSplit.serverAndSegments.serverGrpcPort.toString)

        // Optional: Authorization header if provided
        AuthUtils.buildAuthHeader(readOptions.authHeader, readOptions.authToken) match {
          case Some((name, value)) =>
            val key = Metadata.Key.of(name.toLowerCase, Metadata.ASCII_STRING_MARSHALLER)
            md.put(key, value)
          case None =>
        }
        baseStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
      } else baseStub
    withProxyHeaders
  }

  def fetchData(): Iterator[DataTable] = {
    val request = ServerRequest.newBuilder()
      .putMetadata("enableStreaming", "true")
      // Also include forwarding info in request metadata for compatibility with proxies that inspect payload
      .putMetadata("FORWARD_HOST", pinotSplit.serverAndSegments.serverHost)
      .putMetadata("FORWARD_PORT", pinotSplit.serverAndSegments.serverGrpcPort.toString)
      .addAllSegments(pinotSplit.serverAndSegments.segments.asJava)
      .setSql(
        pinotSplit.serverAndSegments.serverType match {
          case TableType.OFFLINE =>
            pinotSplit.query.offlineSelectQuery
          case TableType.REALTIME =>
            pinotSplit.query.realtimeSelectQuery
        }
      )
      .build()
    val serverResponse = pinotServerBlockingStub.submit(request)
    try {
      val dataTables = for {
        serverResponse <- serverResponse.asScala
        if serverResponse.getMetadataMap.get("responseType") == "data"
      } yield DataTableFactory.getDataTable(serverResponse.getPayload.toByteArray)

      dataTables.filter(_.getNumberOfRows > 0)

    } catch {
      case e: io.grpc.StatusRuntimeException =>
        logError(s"Caught exception when reading data from ${pinotSplit.serverAndSegments.serverHost}:${pinotSplit.serverAndSegments.serverGrpcPort}: ${e}")
        throw e
    }
  }

  def close(): Unit = {
    if (!channel.isShutdown) {
      channel.shutdown()
      logInfo("Pinot server connection closed")
    }
  }
}

object PinotGrpcServerDataFetcher {
  def apply(pinotSplit: PinotSplit, readOptions: PinotDataSourceReadOptions): PinotGrpcServerDataFetcher = {
    new PinotGrpcServerDataFetcher(pinotSplit, readOptions)
  }
}
