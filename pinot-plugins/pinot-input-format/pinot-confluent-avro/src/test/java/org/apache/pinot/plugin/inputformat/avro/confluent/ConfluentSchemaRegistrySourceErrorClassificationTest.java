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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLHandshakeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.classify;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_SCHEMA_REGISTRY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/// Pins the schema-registry rules through Pinot's real decoder and the shipped registry client, against a local
/// registry stub. The serde wraps a registry 401/403 in the same Kafka classes a broker uses, so these failures must
/// name the registry, not the broker.
public class ConfluentSchemaRegistrySourceErrorClassificationTest {
  private static final String TOPIC = "events";
  private static final String VALID_SCHEMA_RESPONSE =
      "{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Event\\\",\\\"fields\\\":"
          + "[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\"}]}\"}";

  private HttpServer _registry;

  @AfterMethod
  public void stopRegistry() {
    if (_registry != null) {
      _registry.stop(0);
      _registry = null;
    }
  }

  @Test
  public void registryRejectsTheCredential()
      throws Exception {
    assertAuth(decodeFailure(registryAnswering(401, "{\"error_code\":401,\"message\":\"Unauthorized\"}"),
        payload(1, new byte[]{2})), IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_SCHEMA_REGISTRY);
  }

  @Test
  public void registryDeniesTheSubject()
      throws Exception {
    String denied = "{\"error_code\":40301,\"message\":\"User is denied operation on this server.\"}";
    assertAuth(decodeFailure(registryAnswering(403, denied), payload(1, new byte[]{2})),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_SCHEMA_REGISTRY);
  }

  @Test
  public void networkPolicyInFrontOfTheRegistryDenies()
      throws Exception {
    // A proxy or firewall answering 403 with an HTML body: the client's error code is 50005, its status stays 403.
    assertAuth(decodeFailure(registryAnswering(403, "<html><body>Forbidden</body></html>"), payload(1, new byte[]{2})),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_SCHEMA_REGISTRY);
  }

  @Test
  public void registryCannotServeTheSchema()
      throws Exception {
    assertSourceRead(decodeFailure(registryAnswering(404, "{\"error_code\":40403,\"message\":\"Schema 1 not found\"}"),
        payload(1, new byte[]{2})), TARGET_SCHEMA_REGISTRY);
    stopRegistry();
    // A throttled registry: the serde maps 429 to a Kafka class and drops the cause.
    String throttled = "{\"error_code\":42901,\"message\":\"Rate limit exceeded\"}";
    assertSourceRead(decodeFailure(registryAnswering(429, throttled), payload(1, new byte[]{2})),
        TARGET_SCHEMA_REGISTRY);
    stopRegistry();
    assertSourceRead(decodeFailure(registryAnswering(503, "{\"error_code\":50301,\"message\":\"Unavailable\"}"),
        payload(1, new byte[]{2})), TARGET_SCHEMA_REGISTRY);
    stopRegistry();
    // A Protobuf reference that was never registered.
    String subjectNotFound = "{\"error_code\":40401,\"message\":\"Subject not found\"}";
    assertSourceRead(decodeFailure(registryAnswering(404, subjectNotFound), payload(1, new byte[]{2})),
        TARGET_SCHEMA_REGISTRY);
    stopRegistry();
    // A URL that isn't a registry at all.
    assertSourceRead(decodeFailure(registryAnswering(404, "<html><body>Not Found</body></html>"),
        payload(1, new byte[]{2})), TARGET_SCHEMA_REGISTRY);
  }

  @Test
  public void registryThatNeverAnswersIsSourceRead()
      throws Exception {
    _registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _registry.createContext("/", exchange -> {
      try {
        Thread.sleep(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.close();
    });
    _registry.start();
    Throwable failure = decodeFailure("http://localhost:" + _registry.getAddress().getPort(),
        payload(1, new byte[]{2}), Map.of("http.read.timeout.ms", "200"));
    assertEquals(classify(failure).getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
  }

  @Test
  public void unreachableRegistryIsSourceRead()
      throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    Throwable failure = decodeFailure("http://localhost:" + closedPort, payload(1, new byte[]{2}));
    assertEquals(classify(failure).getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
  }

  @Test
  public void contentThatWontParseIsNotTheSource()
      throws Exception {
    // Not registry-framed at all.
    assertNotClassified(decodeFailure(registryAnswering(200, VALID_SCHEMA_RESPONSE), new byte[]{1, 2, 3, 4, 5, 6}));
    stopRegistry();
    // The schema is served but the bytes don't match it.
    assertNotClassified(decodeFailure(registryAnswering(200, VALID_SCHEMA_RESPONSE),
        payload(1, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff})));
  }

  @Test
  public void ourTlsOrConfigFailureIsClassifiedByItsCause() {
    // We couldn't verify the registry's certificate: the endpoint, never our credential.
    Throwable pkix = new SerializationException("Error retrieving Avro value schema for id 1",
        new SSLHandshakeException("PKIX path building failed",
            new CertificateException("unable to find valid certification path to requested target")));
    assertEquals(classify(pkix).getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
    // A decoder config the client rejects before any request, and a client class missing from the plugin.
    Map<String, String> badSsl = new HashMap<>();
    badSsl.put("schema.registry.rest.url", "https://localhost:8081");
    badSsl.put("schema.registry.ssl.keystore.location", "/nonexistent/keystore.jks");
    badSsl.put("schema.registry.ssl.keystore.password", "fixture-only");
    try {
      new KafkaConfluentSchemaRegistryAvroMessageDecoder().init(badSsl, Set.of("id"), TOPIC);
      fail("Expected the decoder init to fail");
    } catch (Exception e) {
      assertNotClassified(new RuntimeException("Pinot wrapper", e));
    }
    assertNotClassified(new NoClassDefFoundError(
        "org/apache/kafka/common/security/oauthbearer/JwtRetriever"));
  }

  private String registryAnswering(int status, String body)
      throws Exception {
    _registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _registry.createContext("/", exchange -> {
      byte[] response = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type",
          body.startsWith("<") ? "text/html" : "application/vnd.schemaregistry.v1+json");
      exchange.sendResponseHeaders(status, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    });
    _registry.start();
    return "http://localhost:" + _registry.getAddress().getPort();
  }

  /// A registry-framed payload: magic byte 0, the schema id, then the Avro body.
  private static byte[] payload(int schemaId, byte[] body) {
    return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(schemaId).put(body).array();
  }

  private static Throwable decodeFailure(String registryUrl, byte[] payload)
      throws Exception {
    return decodeFailure(registryUrl, payload, Map.of());
  }

  private static Throwable decodeFailure(String registryUrl, byte[] payload, Map<String, String> clientConfig)
      throws Exception {
    Map<String, String> props = new HashMap<>(clientConfig);
    props.put("schema.registry.rest.url", registryUrl);
    props.put("max.retries", "0");
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    decoder.init(props, Set.of("id"), TOPIC);
    try {
      decoder.decode(payload, new GenericRow());
      fail("Expected the decode to fail");
      return null;
    } catch (RuntimeException e) {
      return e;
    }
  }
}
