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
package org.apache.pinot.connector.spark.common.partition

import org.apache.pinot.connector.spark.common.query.ScanQuery
import org.apache.pinot.connector.spark.common.{InstanceInfo, Logging, PinotDataSourceReadOptions}
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
 * If `segmentsPerSplit` is equal to 1, total 6 spark partition will be created like below,
 *    - partition1: realtimeServer1 -> segment1
 *    - partition2: realtimeServer1 -> segment2
 *    - partition3: realtimeServer1 -> segment3
 *    - partition4: realtimeServer2 -> segment4
 *    - partition5: offlineServer10 -> segment10
 *    - partition6: offlineServer10 -> segment20
 */
private[pinot] object PinotSplitter extends Logging {

  def generatePinotSplits(
      query: ScanQuery,
      routingTable: Map[TableType, Map[String, List[String]]],
      instanceInfoReader: String => InstanceInfo,
      readParameters: PinotDataSourceReadOptions): List[PinotSplit] = {
    routingTable.flatMap {
      case (tableType, serversToSegments) =>
        serversToSegments
          .map { case (server, segments) => (instanceInfoReader(server), segments) }
          .flatMap {
            case (instanceInfo, segments) =>
              createPinotSplitsFromSubSplits(
                tableType,
                query,
                instanceInfo,
                segments,
                readParameters.segmentsPerSplit)
          }
    }.toList
  }

  private def createPinotSplitsFromSubSplits(
      tableType: TableType,
      query: ScanQuery,
      instanceInfo: InstanceInfo,
      segments: List[String],
      segmentsPerSplit: Int): Iterator[PinotSplit] = {
    val maxSegmentCount = Math.min(segments.size, segmentsPerSplit)
    segments.grouped(maxSegmentCount).map { subSegments =>
      val serverAndSegments = {
        PinotServerAndSegments(instanceInfo.hostName,
          instanceInfo.port,
          instanceInfo.grpcPort,
          subSegments,
          tableType)
      }
      PinotSplit(query, serverAndSegments)
    }
  }
}

private[pinot] case class PinotSplit(
    query: ScanQuery,
    serverAndSegments: PinotServerAndSegments)

private[pinot] case class PinotServerAndSegments(
    serverHost: String,
    serverPort: String,
    serverGrpcPort: Int,
    segments: List[String],
    serverType: TableType) {

  override def toString: String = s"$serverHost:$serverPort($serverType)"
}
