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

import io.grpc.ManagedChannelBuilder
import org.apache.pinot.common.datatable.{DataTable, DataTableFactory}
import org.apache.pinot.common.proto.PinotQueryServerGrpc
import org.apache.pinot.common.proto.Server.ServerRequest
import org.apache.pinot.connector.spark.common.Logging
import org.apache.pinot.connector.spark.common.partition.PinotSplit
import org.apache.pinot.spi.config.table.TableType

import java.io.Closeable
import scala.collection.JavaConverters._

/**
 * Data fetcher from Pinot Grpc server for specific segments.
 * Eg: offline-server1: segment1, segment2, segment3
 */
private[reader] class PinotGrpcServerDataFetcher(pinotSplit: PinotSplit)
  extends Logging with Closeable {

  private val channel = ManagedChannelBuilder
    .forAddress(pinotSplit.serverAndSegments.serverHost, pinotSplit.serverAndSegments.serverGrpcPort)
    .usePlaintext()
    .maxInboundMessageSize(Int.MaxValue)
    .asInstanceOf[ManagedChannelBuilder[_]].build()
  private val pinotServerBlockingStub = PinotQueryServerGrpc.newBlockingStub(channel)

  def fetchData(): Iterator[DataTable] = {
    val request = ServerRequest.newBuilder()
      .putMetadata("enableStreaming", "true")
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
  def apply(pinotSplit: PinotSplit): PinotGrpcServerDataFetcher = {
    new PinotGrpcServerDataFetcher(pinotSplit)
  }
}
