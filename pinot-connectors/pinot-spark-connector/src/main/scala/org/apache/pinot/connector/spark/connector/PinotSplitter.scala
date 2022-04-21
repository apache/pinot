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
package org.apache.pinot.connector.spark.connector

import java.util.regex.{Matcher, Pattern}

import org.apache.pinot.connector.spark.connector.query.GeneratedSQLs
import org.apache.pinot.connector.spark.datasource.PinotDataSourceReadOptions
import org.apache.pinot.connector.spark.exceptions.PinotException
import org.apache.pinot.connector.spark.utils.Logging
import org.apache.pinot.spi.config.table.TableType

/**
 * Splitter to create Pinot splits from routing table for spark partitions.
 * Each splits contains one server and list of segments. Each split is moving from driver to executor side.
 * The number of parallelism(partition count) can be decided with `segmentsPerSplit` parameter.
 *
 * Eg:
 * Example routing table;
 *    - realtime ->
 *          - realtimeServer1 -> (segment1, segment2, segment3)
 *          - realtimeServer2 -> (segment4)
 *    - offline ->
 *          - offlineServer10 -> (segment10, segment20)
 *
 *  If `segmentsPerSplit` is equal to 1, total 6 spark partition will be created like below,
 *    - partition1: realtimeServer1 -> segment1
 *    - partition2: realtimeServer1 -> segment2
 *    - partition3: realtimeServer1 -> segment3
 *    - partition4: realtimeServer2 -> segment4
 *    - partition5: offlineServer10 -> segment10
 *    - partition6: offlineServer10 -> segment20
 */
private[pinot] object PinotSplitter extends Logging {
  private val PINOT_SERVER_PATTERN = Pattern.compile("Server_(.*)_(\\d+)")

  def generatePinotSplits(
      generatedSQLs: GeneratedSQLs,
      routingTable: Map[TableType, Map[String, List[String]]],
      grpcPortReader: String => Int,
      readParameters: PinotDataSourceReadOptions): List[PinotSplit] = {
    routingTable.flatMap {
      case (tableType, serversToSegments) =>
        serversToSegments
          .map { case (server, segments) => parseServerInput(server, segments) }
          .flatMap {
            case (matcher, server, segments) =>
              createPinotSplitsFromSubSplits(
                tableType,
                generatedSQLs,
                matcher,
                if (readParameters.useGrpcServer) grpcPortReader(server) else 0,
                segments,
                readParameters.segmentsPerSplit)
          }
    }.toList
  }

  private def parseServerInput(server: String, segments: List[String]): (Matcher, String, List[String]) = {
    val matcher = PINOT_SERVER_PATTERN.matcher(server)
    if (matcher.matches() && matcher.groupCount() == 2) (matcher, server, segments)
    else throw PinotException(s"'$server' did not match!?")
  }

  private def createPinotSplitsFromSubSplits(
      tableType: TableType,
      generatedSQLs: GeneratedSQLs,
      serverMatcher: Matcher,
      grpcPort: Int,
      segments: List[String],
      segmentsPerSplit: Int): Iterator[PinotSplit] = {
    val serverHost = serverMatcher.group(1)
    val serverPort = serverMatcher.group(2)
    val maxSegmentCount = Math.min(segments.size, segmentsPerSplit)
    segments.grouped(maxSegmentCount).map { subSegments =>
      val serverAndSegments = {
        PinotServerAndSegments(serverHost, serverPort, grpcPort, subSegments, tableType)
      }
      PinotSplit(generatedSQLs, serverAndSegments)
    }
  }
}

private[pinot] case class PinotSplit(
    generatedSQLs: GeneratedSQLs,
    serverAndSegments: PinotServerAndSegments)

private[pinot] case class PinotServerAndSegments(
    serverHost: String,
    serverPort: String,
    serverGrpcPort: Int,
    segments: List[String],
    serverType: TableType) {
  override def toString: String = s"$serverHost:$serverPort($serverType)"
}
