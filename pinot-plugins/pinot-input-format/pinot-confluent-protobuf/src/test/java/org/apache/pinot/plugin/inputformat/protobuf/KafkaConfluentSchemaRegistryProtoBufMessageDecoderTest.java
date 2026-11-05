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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RetriableDecodeException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;


/// Tests which schema registry failures make [KafkaConfluentSchemaRegistryProtoBufMessageDecoder] throw
/// [RetriableDecodeException], both on the exception shapes the Confluent deserializer produces and end to end against
/// a fake registry.
public class KafkaConfluentSchemaRegistryProtoBufMessageDecoderTest {
  private static final String TOPIC = "testTopic";

  private HttpServer _registry;
  private volatile int _registryStatus;
  private volatile String _registryBody;

  @BeforeClass
  public void setUp()
      throws IOException {
    _registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _registry.createContext("/", exchange -> {
      byte[] body = _registryBody.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
      exchange.sendResponseHeaders(_registryStatus, body.length);
      try (OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(body);
      }
    });
    _registry.start();
  }

  @AfterClass
  public void tearDown() {
    _registry.stop(0);
  }

  @DataProvider
  public Object[][] failures() {
    String message = "Error retrieving Protobuf schema for id 1";
    RuntimeException cycleStart = new RuntimeException("start");
    cycleStart.initCause(new RuntimeException("end", cycleStart));
    return new Object[][]{
        // HTTP 408, 500, 503, 504 and socket timeouts
        {new TimeoutException(message, new RestClientException("Service unavailable", 503, 50301)), true},
        // HTTP 502
        {new DisconnectException(message, new RestClientException("Bad gateway", 502, 50201)), true},
        // HTTP 429, mapped without a cause
        {new ThrottlingQuotaExceededException("Too many requests"), true},
        {new SerializationException(message, new RestClientException("Too many requests", 429, 42901)), true},
        // The registry could not be reached
        {new SerializationException(message, new ConnectException("Connection refused")), true},
        {new SerializationException(message, new UnknownHostException("registry")), true},
        {new SerializationException(message, new SocketTimeoutException("Read timed out")), true},
        {new SerializationException(message, new RestClientException("Schema 1 not found", 404, 40403)), false},
        {new AuthorizationException(message, new RestClientException("Forbidden", 403, 40301)), false},
        {new SerializationException(message, new RestClientException("Not implemented", 501, 50101)), false},
        {new SerializationException("Unknown magic byte!"), false},
        // A malformed payload: its IOException is not a network failure
        {new SerializationException("Error deserializing Protobuf message for id 1", new EOFException()), false},
        {cycleStart, false}
    };
  }

  @Test(dataProvider = "failures")
  public void testIsTransientSchemaRegistryFailure(RuntimeException failure, boolean expectedTransient) {
    assertEquals(KafkaConfluentSchemaRegistryProtoBufMessageDecoder.isTransientSchemaRegistryFailure(failure),
        expectedTransient);
  }

  @Test
  public void testRegistryServerErrorIsRetriable()
      throws Exception {
    respondWith(503, "{\"error_code\":50301,\"message\":\"Service unavailable\"}");
    byte[] payload = payload();
    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder = createDecoder(registryUrl());
    expectThrows(RetriableDecodeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
  }

  @Test
  public void testUnreachableRegistryIsRetriable()
      throws Exception {
    int closedPort;
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      closedPort = serverSocket.getLocalPort();
    }
    byte[] payload = payload();
    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder = createDecoder("http://localhost:" + closedPort);
    expectThrows(RetriableDecodeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
  }

  @Test
  public void testUnknownSchemaIsNotRetriable()
      throws Exception {
    respondWith(404, "{\"error_code\":40403,\"message\":\"Schema 1 not found\"}");
    byte[] payload = payload();
    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder = createDecoder(registryUrl());
    RuntimeException e =
        expectThrows(RuntimeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
    assertFalse(e instanceof RetriableDecodeException, e.toString());
  }

  private void respondWith(int status, String body) {
    _registryStatus = status;
    _registryBody = body;
  }

  private String registryUrl() {
    return "http://localhost:" + _registry.getAddress().getPort();
  }

  private static KafkaConfluentSchemaRegistryProtoBufMessageDecoder createDecoder(String registryUrl)
      throws Exception {
    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder =
        new KafkaConfluentSchemaRegistryProtoBufMessageDecoder();
    // No retries inside the registry client, which would only slow the test down
    decoder.init(Map.of("schema.registry.rest.url", registryUrl, "max.retries", "0"), Set.of("name"), TOPIC);
    return decoder;
  }

  /// A message in the Confluent wire format for schema id 1: the registry is asked for the schema before the body is
  /// read.
  private static byte[] payload()
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(0);
    outputStream.write(ByteBuffer.allocate(4).putInt(1).array());
    outputStream.write(new byte[]{0});
    return outputStream.toByteArray();
  }
}
