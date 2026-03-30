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
package org.apache.pinot.client.grpc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.client.AbstractJdbcDriverAuthTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for {@code jdbc:pinotgrpc://} driver path through {@link java.sql.DriverManager}.
 *
 * <p>Verifies that auth credentials and gRPC metadata options (blockRowSize, encoding,
 * compression) are correctly propagated through the JDBC driver into
 * {@link PinotGrpcConnection#getMetadataMap()}.
 *
 * <p>Uses the mock gRPC server from {@link AbstractJdbcDriverAuthTest} to handle
 * the validation query that the gRPC connection sends on construction.
 *
 * <p>The gRPC path extracts {@code headers.*} prefixed properties (same behavior as HTTP path),
 * providing consistent header propagation across both JDBC driver variants.
 */
@Test
public class GrpcJdbcDriverAuthTest extends AbstractJdbcDriverAuthTest {

  @Override
  protected String getJdbcUrl() {
    // Controller URL — not actually contacted since we set "brokers" property
    return "jdbc:pinotgrpc://localhost:" + _mockGrpcPort;
  }

  @Override
  protected Properties getBaseProperties() {
    Properties props = new Properties();
    // Point brokers directly to the mock gRPC server so the validation query succeeds
    props.setProperty("brokers", "localhost:" + _mockGrpcPort);
    return props;
  }

  @Override
  protected Map<String, String> extractHeadersFromConnection(Connection conn)
      throws Exception {
    PinotGrpcConnection grpcConn = (PinotGrpcConnection) conn;
    return grpcConn.getMetadataMap();
  }

  @Test
  public void testGrpcOptionsBlockRowSizeEncodingCompression()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("Authorization", "Basic token");
    props.setProperty("blockRowSize", "10000");
    props.setProperty("encoding", "JSON");
    props.setProperty("compression", "ZSTD");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> metadata = extractHeadersFromConnection(conn);

      assertEquals(metadata.get("blockRowSize"), "10000",
          "blockRowSize should be propagated into metadata map");
      assertEquals(metadata.get("encoding"), "JSON",
          "encoding should be propagated into metadata map");
      assertEquals(metadata.get("compression"), "ZSTD",
          "compression should be propagated into metadata map");
    }
  }
}
