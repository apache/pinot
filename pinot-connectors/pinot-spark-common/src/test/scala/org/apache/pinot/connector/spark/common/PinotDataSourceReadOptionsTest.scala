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

import scala.collection.JavaConverters._

/**
 * Test datasource read configs and defaults values.
 */
class PinotDataSourceReadOptionsTest extends BaseTest {

  test("Spark DataSourceOptions should be converted to the PinotDataSourceReadOptions") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "hybrid",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_SEGMENTS_PER_SPLIT -> "1",
      PinotDataSourceReadOptions.CONFIG_USE_PUSH_DOWN_FILTERS -> "false",
      PinotDataSourceReadOptions.CONFIG_USE_GRPC_SERVER -> "false",
      PinotDataSourceReadOptions.CONFIG_QUERY_OPTIONS -> "a=1,b=2"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    val expected =
      PinotDataSourceReadOptions(
        "tbl",
        None,
        "localhost:9000",
        "localhost:8000",
        usePushDownFilters = false,
        1,
        10000,
        useGrpcServer = false,
        Set("a=1", "b=2"),
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

    pinotDataSourceReadOptions shouldEqual expected
  }

  test("HTTPS configuration options should be parsed correctly") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_USE_HTTPS -> "false"  // Don't enable HTTPS to avoid early configuration
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    // Test that HTTPS defaults work correctly
    pinotDataSourceReadOptions.useHttps shouldEqual false
    pinotDataSourceReadOptions.keystorePath shouldEqual None
    pinotDataSourceReadOptions.keystorePassword shouldEqual None
    pinotDataSourceReadOptions.truststorePath shouldEqual None
    pinotDataSourceReadOptions.truststorePassword shouldEqual None
  }

  test("HTTPS configuration should default to false with empty optional values") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.useHttps shouldEqual false
    pinotDataSourceReadOptions.keystorePath shouldEqual None
    pinotDataSourceReadOptions.keystorePassword shouldEqual None
    pinotDataSourceReadOptions.truststorePath shouldEqual None
    pinotDataSourceReadOptions.truststorePassword shouldEqual None
  }

  test("Empty HTTPS configuration values should be filtered out") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_USE_HTTPS -> "true",
      PinotDataSourceReadOptions.CONFIG_KEYSTORE_PATH -> "",
      PinotDataSourceReadOptions.CONFIG_KEYSTORE_PASSWORD -> "",
      PinotDataSourceReadOptions.CONFIG_TRUSTSTORE_PATH -> "",
      PinotDataSourceReadOptions.CONFIG_TRUSTSTORE_PASSWORD -> ""
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.useHttps shouldEqual true
    pinotDataSourceReadOptions.keystorePath shouldEqual None
    pinotDataSourceReadOptions.keystorePassword shouldEqual None
    pinotDataSourceReadOptions.truststorePath shouldEqual None
    pinotDataSourceReadOptions.truststorePassword shouldEqual None
  }

  test("Authentication header configuration should be parsed correctly") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_AUTH_HEADER -> "Authorization",
      PinotDataSourceReadOptions.CONFIG_AUTH_TOKEN -> "Bearer my-token"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.authHeader shouldEqual Some("Authorization")
    pinotDataSourceReadOptions.authToken shouldEqual Some("Bearer my-token")
  }

  test("Authentication should default to empty when not provided") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.authHeader shouldEqual None
    pinotDataSourceReadOptions.authToken shouldEqual None
  }

  test("Empty authentication values should be filtered out") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_AUTH_HEADER -> "",
      PinotDataSourceReadOptions.CONFIG_AUTH_TOKEN -> ""
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.authHeader shouldEqual None
    pinotDataSourceReadOptions.authToken shouldEqual None
  }

  test("Proxy configuration options should be parsed correctly") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_PROXY_ENABLED -> "true"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.proxyEnabled shouldEqual true
  }

  test("Proxy should default to false when not provided") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.proxyEnabled shouldEqual false
  }

  test("gRPC configuration options should be parsed correctly") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_GRPC_PORT -> "8091",
      PinotDataSourceReadOptions.CONFIG_GRPC_MAX_INBOUND_MESSAGE_SIZE -> "256000000",
      PinotDataSourceReadOptions.CONFIG_GRPC_USE_PLAIN_TEXT -> "false",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_KEYSTORE_TYPE -> "PKCS12",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_KEYSTORE_PATH -> "/path/to/grpc/keystore.p12",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_KEYSTORE_PASSWORD -> "grpc-keystore-pass",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_TRUSTSTORE_TYPE -> "PKCS12",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_TRUSTSTORE_PATH -> "/path/to/grpc/truststore.p12",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_TRUSTSTORE_PASSWORD -> "grpc-truststore-pass",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_SSL_PROVIDER -> "OPENSSL",
      PinotDataSourceReadOptions.CONFIG_GRPC_PROXY_URI -> "proxy-host:8094"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.grpcPort shouldEqual 8091
    pinotDataSourceReadOptions.grpcMaxInboundMessageSize shouldEqual 256000000L
    pinotDataSourceReadOptions.grpcUsePlainText shouldEqual false
    pinotDataSourceReadOptions.grpcTlsKeystoreType shouldEqual "PKCS12"
    pinotDataSourceReadOptions.grpcTlsKeystorePath shouldEqual Some("/path/to/grpc/keystore.p12")
    pinotDataSourceReadOptions.grpcTlsKeystorePassword shouldEqual Some("grpc-keystore-pass")
    pinotDataSourceReadOptions.grpcTlsTruststoreType shouldEqual "PKCS12"
    pinotDataSourceReadOptions.grpcTlsTruststorePath shouldEqual Some("/path/to/grpc/truststore.p12")
    pinotDataSourceReadOptions.grpcTlsTruststorePassword shouldEqual Some("grpc-truststore-pass")
    pinotDataSourceReadOptions.grpcTlsSslProvider shouldEqual "OPENSSL"
    pinotDataSourceReadOptions.grpcProxyUri shouldEqual Some("proxy-host:8094")
  }

  test("gRPC configuration should use default values when not provided") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.grpcPort shouldEqual 8090
    pinotDataSourceReadOptions.grpcMaxInboundMessageSize shouldEqual 134217728L // 128MB
    pinotDataSourceReadOptions.grpcUsePlainText shouldEqual true
    pinotDataSourceReadOptions.grpcTlsKeystoreType shouldEqual "JKS"
    pinotDataSourceReadOptions.grpcTlsKeystorePath shouldEqual None
    pinotDataSourceReadOptions.grpcTlsKeystorePassword shouldEqual None
    pinotDataSourceReadOptions.grpcTlsTruststoreType shouldEqual "JKS"
    pinotDataSourceReadOptions.grpcTlsTruststorePath shouldEqual None
    pinotDataSourceReadOptions.grpcTlsTruststorePassword shouldEqual None
    pinotDataSourceReadOptions.grpcTlsSslProvider shouldEqual "JDK"
    pinotDataSourceReadOptions.grpcProxyUri shouldEqual None
  }

  test("Empty gRPC configuration values should be filtered out") {
    val options = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offline",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_KEYSTORE_PATH -> "",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_KEYSTORE_PASSWORD -> "",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_TRUSTSTORE_PATH -> "",
      PinotDataSourceReadOptions.CONFIG_GRPC_TLS_TRUSTSTORE_PASSWORD -> "",
      PinotDataSourceReadOptions.CONFIG_GRPC_PROXY_URI -> ""
    )

    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(options.asJava)

    pinotDataSourceReadOptions.grpcTlsKeystorePath shouldEqual None
    pinotDataSourceReadOptions.grpcTlsKeystorePassword shouldEqual None
    pinotDataSourceReadOptions.grpcTlsTruststorePath shouldEqual None
    pinotDataSourceReadOptions.grpcTlsTruststorePassword shouldEqual None
    pinotDataSourceReadOptions.grpcProxyUri shouldEqual None
  }

  test("Method should throw exception if `tableType` option is missing or wrong") {
    // missing
    val missingOption = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    // wrong input
    val wrongOption = Map(
      PinotDataSourceReadOptions.CONFIG_TABLE_NAME -> "tbl",
      PinotDataSourceReadOptions.CONFIG_TABLE_TYPE -> "offlinee",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000"
    )

    val missingException = intercept[PinotException] {
      PinotDataSourceReadOptions.from(missingOption.asJava)
    }

    val wrongException = intercept[PinotException] {
      PinotDataSourceReadOptions.from(wrongOption.asJava)
    }

    missingException.getMessage shouldEqual "`tableType` should be specified"
    wrongException.getMessage shouldEqual "Unknown `tableType`: OFFLINEE"
  }

}
