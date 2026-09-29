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

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.io.OutputStream
import java.net.InetSocketAddress

class PinotClusterClientTest extends BaseTest {

  test("getBrokerInstances should successfully parse livebrokers API response") {
    // Create a test HTTP server to provide controlled response
    val testServer = HttpServer.create(new InetSocketAddress(0), 0)
    val serverPort = testServer.getAddress.getPort

    val tableName = "testTable"
    val mockResponse = s"""{
      "$tableName": [
        {
          "host": "dca24-3fs",
          "port": 29041,
          "instanceName": "Broker_abfa3b77-a1e7-4525-97f9-3114898606e5"
        },
        {
          "host": "dca50-a72",
          "port": 25607,
          "instanceName": "Broker_e41eecea-4f3d-4528-9510-7e63e282b441"
        }
      ]
    }"""

    testServer.createContext("/tables/livebrokers", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val query = exchange.getRequestURI.getQuery
        if (query != null && query.contains(s"tables=$tableName")) {
          val response = mockResponse.getBytes("UTF-8")
          exchange.sendResponseHeaders(200, response.length)
          val os: OutputStream = exchange.getResponseBody
          os.write(response)
          os.close()
        } else {
          exchange.sendResponseHeaders(404, 0)
          exchange.getResponseBody.close()
        }
      }
    })

    testServer.start()

    try {
      val controllerUrl = s"localhost:$serverPort"
      val brokerUrls = PinotClusterClient.getBrokerInstances(controllerUrl, tableName)

      brokerUrls should have size 2
      brokerUrls should contain("dca24-3fs:29041")
      brokerUrls should contain("dca50-a72:25607")

    } finally {
      testServer.stop(0)
    }
  }

  test("getBrokerInstances should throw PinotException on HttpUtils failure") {
    // Test that getBrokerInstances handles failures by using an unreachable endpoint
    val tableName = "testTable"
    val controllerUrl = "localhost:99999"  // Unreachable port

    val exception = intercept[PinotException] {
      PinotClusterClient.getBrokerInstances(controllerUrl, tableName)
    }

    // Verify the exception message
    exception.getMessage should include(s"An error occurred while getting broker instances for table '$tableName'")
  }
}
