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

import org.apache.pinot.connector.spark.common.partition.{PinotServerAndSegments, PinotSplit, PinotSplitter}
import org.apache.pinot.connector.spark.common.query.ScanQuery
import org.apache.pinot.spi.config.table.TableType

import java.util.regex.Pattern

/**
 * Test num of Spark partitions by routing table and input configs.
 */
class PinotSplitterTest extends BaseTest {
  private val query = ScanQuery("tbl", None, "", "")
  private val mockInstanceInfoReader = (server: String) => {
    val matcher = Pattern.compile("Server_(.*)_(\\d+)").matcher(server)
    matcher.matches()
    InstanceInfo(server, matcher.group(1), matcher.group(2), -1)
  }

  private val routingTable = Map(
    TableType.OFFLINE -> Map(
      "Server_192.168.1.100_7000" -> List("segment1", "segment2", "segment3"),
      "Server_192.168.2.100_9000" -> List("segment4"),
      "Server_192.168.3.100_7000" -> List("segment5", "segment6")
    ),
    TableType.REALTIME -> Map(
      "Server_192.168.33.100_5000" -> List("segment10", "segment11", "segment12"),
      "Server_192.168.44.100_7000" -> List("segment13")
    )
  )

  private val getReadOptionsWithSegmentsPerSplit = (segmentsPerSplit: Int) => {
    new PinotDataSourceReadOptions(
      "tableName",
      Option(TableType.OFFLINE),
      "controller",
      "broker",
      false,
      segmentsPerSplit,
      1000,
      false,
      Set())
  }

  test("Total 5 partition splits should be created for maxNumSegmentPerServerRequest = 3") {
    val readOptions = getReadOptionsWithSegmentsPerSplit(3)
    val splitResults =
      PinotSplitter.generatePinotSplits(query, routingTable, mockInstanceInfoReader, readOptions)

    splitResults.size shouldEqual 5
  }

  test("Total 5 partition splits should be created for maxNumSegmentPerServerRequest = 90") {
    val readOptions = getReadOptionsWithSegmentsPerSplit(90)
    val splitResults =
      PinotSplitter.generatePinotSplits(query, routingTable, mockInstanceInfoReader, readOptions)

    splitResults.size shouldEqual 5
  }

  test("Total 10 partition splits should be created for maxNumSegmentPerServerRequest = 1") {
    val readOptions = getReadOptionsWithSegmentsPerSplit(1)
    val splitResults =
      PinotSplitter.generatePinotSplits(query, routingTable, mockInstanceInfoReader, readOptions)

    splitResults.size shouldEqual 10
  }

  test("Input pinot server string should be parsed successfully") {
    val inputRoutingTable = Map(
      TableType.REALTIME -> Map("Server_192.168.1.100_9000" -> List("segment1"))
    )
    val readOptions = getReadOptionsWithSegmentsPerSplit(5)

    val splitResults = PinotSplitter.generatePinotSplits(query, inputRoutingTable, mockInstanceInfoReader, readOptions)
    val expectedOutput = List(
      PinotSplit(
        query,
        PinotServerAndSegments("192.168.1.100", "9000", -1, List("segment1"), TableType.REALTIME)
      )
    )

    expectedOutput should contain theSameElementsAs splitResults
  }

  test("GeneratePinotSplits with Grpc port reading enabled") {
    val inputRoutingTable = Map(
      TableType.REALTIME -> Map("Server_192.168.1.100_9000" -> List("segment1"))
    )
    val inputReadOptions = new PinotDataSourceReadOptions(
      "tableName",
      Option(TableType.REALTIME),
      "controller",
      "broker",
      false,
      1,
      1000,
      true,
      Set())

    val inputGrpcPortReader = (server: String) => {
      InstanceInfo(server, "192.168.1.100", "9000", 8090)
    }

    val splitResults =
      PinotSplitter.generatePinotSplits(query, inputRoutingTable, inputGrpcPortReader, inputReadOptions)
    val expectedOutput = List(
      PinotSplit(
        query,
        PinotServerAndSegments("192.168.1.100", "9000", 8090, List("segment1"), TableType.REALTIME)
      )
    )

    expectedOutput should contain theSameElementsAs splitResults
  }
}
