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

import org.apache.pinot.spi.config.table.TableType

/**
 * Test gRPC utilities for channel creation and proxy metadata handling.
 */
class GrpcUtilsTest extends BaseTest {

  test("Proxy metadata should be created correctly") {
    val metadata = GrpcUtils.createProxyMetadata("localhost", 8090)
    
    metadata should not be null
    val forwardHost = metadata.get(io.grpc.Metadata.Key.of("FORWARD_HOST", io.grpc.Metadata.ASCII_STRING_MARSHALLER))
    val forwardPort = metadata.get(io.grpc.Metadata.Key.of("FORWARD_PORT", io.grpc.Metadata.ASCII_STRING_MARSHALLER))
    
    forwardHost shouldEqual "localhost"
    forwardPort shouldEqual "8090"
  }

  test("Plain text gRPC channel should be created successfully") {
    val options = PinotDataSourceReadOptions(
      "test_table",
      Some(TableType.OFFLINE),
      "localhost:9000",
      "localhost:8000",
      usePushDownFilters = true,
      3,
      10000,
      useGrpcServer = true,
      Set(),
      failOnInvalidSegments = false,
      useHttps = false,
      keystorePath = None,
      keystorePassword = None,
      truststorePath = None,
      truststorePassword = None,
      authHeader = None,
      authToken = None,
      proxyEnabled = false,
      grpcPort = 8090,
      grpcMaxInboundMessageSize = 134217728L,
      grpcUsePlainText = true,
      grpcTlsKeystoreType = "JKS",
      grpcTlsKeystorePath = None,
      grpcTlsKeystorePassword = None,
      grpcTlsTruststoreType = "JKS",
      grpcTlsTruststorePath = None,
      grpcTlsTruststorePassword = None,
      grpcTlsSslProvider = "JDK",
      grpcProxyUri = None
    )

    val channel = GrpcUtils.createChannel("localhost", 8090, options)
    
    channel should not be null
    channel.isShutdown shouldEqual false
    
    // Clean up
    channel.shutdown()
  }

  test("TLS gRPC channel creation should handle missing keystore gracefully") {
    val options = PinotDataSourceReadOptions(
      "test_table",
      Some(TableType.OFFLINE),
      "localhost:9000",
      "localhost:8000",
      usePushDownFilters = true,
      3,
      10000,
      useGrpcServer = true,
      Set(),
      failOnInvalidSegments = false,
      useHttps = false,
      keystorePath = None,
      keystorePassword = None,
      truststorePath = None,
      truststorePassword = None,
      authHeader = None,
      authToken = None,
      proxyEnabled = false,
      grpcPort = 8090,
      grpcMaxInboundMessageSize = 134217728L,
      grpcUsePlainText = false, // TLS enabled
      grpcTlsKeystoreType = "JKS",
      grpcTlsKeystorePath = None, // No keystore provided
      grpcTlsKeystorePassword = None,
      grpcTlsTruststoreType = "JKS",
      grpcTlsTruststorePath = None, // No truststore provided
      grpcTlsTruststorePassword = None,
      grpcTlsSslProvider = "JDK",
      grpcProxyUri = None
    )

    // This should work - TLS with default trust manager
    val channel = GrpcUtils.createChannel("localhost", 8090, options)
    
    channel should not be null
    channel.isShutdown shouldEqual false
    
    // Clean up
    channel.shutdown()
  }

  test("gRPC channel should throw exception for invalid keystore configuration") {
    val options = PinotDataSourceReadOptions(
      "test_table",
      Some(TableType.OFFLINE),
      "localhost:9000",
      "localhost:8000",
      usePushDownFilters = true,
      3,
      10000,
      useGrpcServer = true,
      Set(),
      failOnInvalidSegments = false,
      useHttps = false,
      keystorePath = None,
      keystorePassword = None,
      truststorePath = None,
      truststorePassword = None,
      authHeader = None,
      authToken = None,
      proxyEnabled = false,
      grpcPort = 8090,
      grpcMaxInboundMessageSize = 134217728L,
      grpcUsePlainText = false, // TLS enabled
      grpcTlsKeystoreType = "JKS",
      grpcTlsKeystorePath = Some("/non/existent/keystore.jks"), // Invalid path
      grpcTlsKeystorePassword = None, // Missing password
      grpcTlsTruststoreType = "JKS",
      grpcTlsTruststorePath = None,
      grpcTlsTruststorePassword = None,
      grpcTlsSslProvider = "JDK",
      grpcProxyUri = None
    )

    val exception = intercept[RuntimeException] {
      GrpcUtils.createChannel("localhost", 8090, options)
    }
    
    exception.getMessage should include("gRPC TLS configuration failed")
    exception.getCause shouldBe a[IllegalArgumentException]
    exception.getCause.getMessage should include("gRPC keystore password is required")
  }
}
