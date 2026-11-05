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
package org.apache.pinot.plugin.inputformat.avro.confluent;

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
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RetriableDecodeException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests which schema registry failures make [KafkaConfluentSchemaRegistryAvroMessageDecoder] throw
/// [RetriableDecodeException], both on the exception shapes the Confluent deserializer produces and end to end against
/// a fake registry.
public class KafkaConfluentSchemaRegistryAvroMessageDecoderTest {
  private static final String TOPIC = "testTopic";
  private static final int SCHEMA_ID = 1;
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

  private HttpServer _registry;
  private volatile int _registryStatus;
  private volatile String _registryBody;
  private final AtomicInteger _registryRequests = new AtomicInteger();

  @BeforeClass
  public void setUp()
      throws IOException {
    _registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _registry.createContext("/", exchange -> {
      _registryRequests.incrementAndGet();
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
  public Object[][] transientFailures() {
    String message = "Error retrieving Avro value schema for id 1";
    return new Object[][]{
        // HTTP 408, 500, 503, 504 and socket timeouts
        {new TimeoutException(message, new RestClientException("Service unavailable", 503, 50301))},
        // HTTP 502
        {new DisconnectException(message, new RestClientException("Bad gateway", 502, 50201))},
        // HTTP 429, mapped without a cause
        {new ThrottlingQuotaExceededException("Too many requests")},
        // Errors wrapped again while reading the message
        {new SerializationException("Error deserializing Avro message for id 1",
            new TimeoutException(message, new SocketTimeoutException("Read timed out")))},
        {new SerializationException("Error deserializing Avro message for id 1",
            new RestClientException("Too many requests", 429, 42901))},
        // The registry could not be reached
        {new SerializationException(message, new ConnectException("Connection refused"))},
        {new SerializationException(message, new UnknownHostException("registry"))},
        {new SerializationException(message, new SocketTimeoutException("connect timed out"))}
    };
  }

  @Test(dataProvider = "transientFailures")
  public void testTransientSchemaRegistryFailure(RuntimeException failure) {
    assertTrue(KafkaConfluentSchemaRegistryAvroMessageDecoder.isTransientSchemaRegistryFailure(failure));
  }

  @DataProvider
  public Object[][] permanentFailures() {
    RuntimeException cycleStart = new RuntimeException("start");
    RuntimeException cycleEnd = new RuntimeException("end", cycleStart);
    cycleStart.initCause(cycleEnd);
    return new Object[][]{
        {new SerializationException("Error retrieving Avro value schema for id 1",
            new RestClientException("Schema 1 not found", 404, 40403))},
        {new AuthorizationException("Error retrieving Avro value schema for id 1",
            new RestClientException("Forbidden", 403, 40301))},
        // Server errors that retrying does not fix
        {new SerializationException("Error retrieving Avro value schema for id 1",
            new RestClientException("Not implemented", 501, 50101))},
        {new SerializationException("Unknown magic byte!")},
        // A malformed payload: its IOException is not a network failure
        {new SerializationException("Error deserializing Avro message for id 1", new EOFException())},
        {new SerializationException("Error deserializing Avro message for id 1",
            new AvroRuntimeException("Malformed data. Length is negative: -1"))},
        {cycleStart}
    };
  }

  @Test(dataProvider = "permanentFailures")
  public void testPermanentFailure(RuntimeException failure) {
    assertFalse(KafkaConfluentSchemaRegistryAvroMessageDecoder.isTransientSchemaRegistryFailure(failure));
  }

  @Test
  public void testRegistryServerErrorIsRetriable()
      throws Exception {
    respondWith(503, "{\"error_code\":50301,\"message\":\"Service unavailable\"}");
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder(registryUrl());
    byte[] payload = encode("Alice");
    // Not cached: every attempt asks the registry again
    for (int attempt = 1; attempt <= 2; attempt++) {
      int requestsBefore = _registryRequests.get();
      expectThrows(RetriableDecodeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
      assertTrue(_registryRequests.get() > requestsBefore);
    }
  }

  @Test
  public void testRegistryThrottlingIsRetriable()
      throws Exception {
    respondWith(429, "{\"error_code\":42901,\"message\":\"Too many requests\"}");
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder(registryUrl());
    byte[] payload = encode("Alice");
    expectThrows(RetriableDecodeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
  }

  @Test
  public void testUnreachableRegistryIsRetriable()
      throws Exception {
    int closedPort;
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      closedPort = serverSocket.getLocalPort();
    }
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder("http://localhost:" + closedPort);
    byte[] payload = encode("Alice");
    RetriableDecodeException e = expectThrows(RetriableDecodeException.class,
        () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
    assertTrue(e.getMessage().contains(TOPIC), e.getMessage());
  }

  @Test
  public void testUnknownSchemaIsNotRetriable()
      throws Exception {
    respondWith(404, "{\"error_code\":40403,\"message\":\"Schema 1 not found\"}");
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder(registryUrl());
    byte[] payload = encode("Alice");
    RuntimeException e =
        expectThrows(RuntimeException.class, () -> decoder.decode(payload, 0, payload.length, new GenericRow()));
    assertFalse(e instanceof RetriableDecodeException, e.toString());
  }

  @Test
  public void testMalformedPayloadIsNotRetriable()
      throws Exception {
    respondWith(200, JsonUtils.objectToString(Map.of("schema", AVRO_SCHEMA.toString())));
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder(registryUrl());
    byte[] payload = encode("Alice");
    GenericRow row = decoder.decode(payload, 0, payload.length, new GenericRow());
    assertEquals(row.getValue("name"), "Alice");
    // The schema resolves, but the record is cut short
    byte[] truncatedPayload = Arrays.copyOf(payload, payload.length - 3);
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> decoder.decode(truncatedPayload, 0, truncatedPayload.length, new GenericRow()));
    assertFalse(e instanceof RetriableDecodeException, e.toString());
  }

  @Test
  public void testUnknownMagicByteIsNotRetriable()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = createDecoder(registryUrl());
    byte[] payload = encode("Alice");
    // Neither 0 nor 1, the magic bytes of the two schema id formats
    payload[0] = 0x7F;
    try {
      // Dropped either way: older Confluent clients report it so that the decoder returns null
      assertNull(decoder.decode(payload, 0, payload.length, new GenericRow()));
    } catch (RuntimeException e) {
      assertFalse(e instanceof RetriableDecodeException, e.toString());
    }
  }

  private void respondWith(int status, String body) {
    _registryStatus = status;
    _registryBody = body;
  }

  private String registryUrl() {
    return "http://localhost:" + _registry.getAddress().getPort();
  }

  private static KafkaConfluentSchemaRegistryAvroMessageDecoder createDecoder(String registryUrl)
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    // No retries inside the registry client, which would only slow the test down
    decoder.init(Map.of("schema.registry.rest.url", registryUrl, "max.retries", "0"), Set.of("name"), TOPIC);
    return decoder;
  }

  /// Encodes a record in the Confluent wire format: magic byte 0, the 4-byte schema id, then the Avro binary.
  private static byte[] encode(String name)
      throws IOException {
    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("name", name);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(0);
    outputStream.write(ByteBuffer.allocate(4).putInt(SCHEMA_ID).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA).write(record, encoder);
    encoder.flush();
    return outputStream.toByteArray();
  }
}
