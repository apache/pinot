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
package org.apache.pinot.plugin.stream.kinesis;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.ingestion.error.IngestionSourceCredentialMode;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.sts.model.StsException;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_STS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.expectThrows;


/// Drives the plugin's real Kinesis client, and for an assumed role its real STS client, against a local stub that
/// answers with AWS's own error codes; LocalStack enforces no IAM. The same credential code is the table's bad key
/// under static keys and Pinot's own identity under the default chain, and only the evidence the plugin attaches tells
/// them apart. The keys are fake, and nothing here prints them.
public class KinesisCredentialEvidenceTest {
  private static final String FAKE_ACCESS_KEY = "FAKEACCESSKEYFORSTUB";
  private static final String FAKE_SECRET_KEY = "fake-secret-key-for-a-local-stub";
  private static final String FAKE_ROLE_ARN = "arn:aws:iam::000000000000:role/fake-table-role";
  // The plugin configures no STS endpoint; the SDK reads this override when it builds the STS client
  private static final String STS_ENDPOINT_PROPERTY = "aws.endpointUrlSts";
  private static final KinesisPartitionGroupOffset START_OFFSET =
      new KinesisPartitionGroupOffset("shardId-000000000000", "1");
  private static final int TIMEOUT_MS = 5000;

  private HttpServer _stub;
  private String _endpoint;
  private volatile String _stsErrorCode;
  private final AtomicInteger _kinesisRequests = new AtomicInteger();
  private final AtomicInteger _stsRequests = new AtomicInteger();

  @BeforeClass
  public void setUp()
      throws IOException {
    _stub = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    _stub.createContext("/", this::answer);
    _stub.start();
    _endpoint = "http://" + _stub.getAddress().getHostString() + ":" + _stub.getAddress().getPort();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (_stub != null) {
      _stub.stop(0);
    }
  }

  @BeforeMethod
  public void resetRequests() {
    _kinesisRequests.set(0);
    _stsRequests.set(0);
  }

  /// Kinesis rejects every request's credential. A Kinesis request names its operation in a header (AWS JSON
  /// protocol); anything else is an STS request (query protocol), answered with the code the test sets.
  private void answer(HttpExchange exchange)
      throws IOException {
    exchange.getRequestBody().readAllBytes();
    if (exchange.getRequestHeaders().containsKey("X-Amz-Target")) {
      _kinesisRequests.incrementAndGet();
      exchange.getResponseHeaders().add("x-amzn-ErrorType", "UnrecognizedClientException");
      respond(exchange, 400, "application/x-amz-json-1.1", "{\"__type\":\"UnrecognizedClientException\","
          + "\"message\":\"The security token included in the request is invalid.\"}");
    } else {
      _stsRequests.incrementAndGet();
      String message = "AccessDenied".equals(_stsErrorCode)
          ? "User: arn:aws:iam::000000000000:user/fake is not authorized to perform: sts:AssumeRole on resource: "
          + "arn:aws:iam::000000000000:role/fake-table-role"
          : "The security token included in the request is invalid.";
      respond(exchange, 403, "text/xml", "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>"
          + "<Type>Sender</Type><Code>" + _stsErrorCode + "</Code><Message>" + message + "</Message></Error>"
          + "<RequestId>00000000-0000-0000-0000-000000000000</RequestId></ErrorResponse>");
    }
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private StreamConfig streamConfig(Map<String, String> credentials) {
    Map<String, String> props = new HashMap<>();
    props.put(StreamConfigProperties.STREAM_TYPE, "kinesis");
    props.put("stream.kinesis.topic.name", "events");
    props.put("stream.kinesis.consumer.factory.class.name", KinesisConsumerFactory.class.getName());
    props.put("stream.kinesis.decoder.class.name", "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    props.put(KinesisConfig.REGION, "us-east-1");
    props.put(KinesisConfig.ENDPOINT, _endpoint);
    props.putAll(credentials);
    return new StreamConfig("events_REALTIME", props);
  }

  @Test
  public void kinesisRejectingTheTablesKeysIsAuth()
      throws Exception {
    StreamConfig streamConfig =
        streamConfig(Map.of(KinesisConfig.ACCESS_KEY, FAKE_ACCESS_KEY, KinesisConfig.SECRET_KEY, FAKE_SECRET_KEY));
    // The server's fetch
    try (KinesisConsumer consumer = new KinesisConsumer(new KinesisConfig(streamConfig))) {
      KinesisException rejected =
          expectThrows(KinesisException.class, () -> consumer.fetchMessages(START_OFFSET, TIMEOUT_MS));
      assertAuth(rejected, IngestionSourceErrorSubReason.AUTHENTICATION, null);
    }
    // The controller's partition metadata, and the server's partition count
    KinesisStreamMetadataProvider metadataProvider = new KinesisStreamMetadataProvider("clientId", streamConfig);
    assertAuth(expectThrows(KinesisException.class,
            () -> metadataProvider.computePartitionGroupMetadata("clientId", streamConfig, List.of(), TIMEOUT_MS)),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(expectThrows(RuntimeException.class, () -> metadataProvider.fetchPartitionCount(TIMEOUT_MS)),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  @Test
  public void kinesisRejectingOurOwnIdentityIsNotTheSource()
      throws Throwable {
    // With no keys on the table the default chain signs with Pinot's own identity; system properties come first in it
    withSystemProperties(Map.of("aws.accessKeyId", FAKE_ACCESS_KEY, "aws.secretAccessKey", FAKE_SECRET_KEY), () -> {
      try (KinesisConsumer consumer = new KinesisConsumer(new KinesisConfig(streamConfig(Map.of())))) {
        KinesisException rejected =
            expectThrows(KinesisException.class, () -> consumer.fetchMessages(START_OFFSET, TIMEOUT_MS));
        assertEquals(IngestionSourceErrorEvidence.of(rejected).getCredentialMode(),
            IngestionSourceCredentialMode.AMBIENT);
        assertNotClassified(rejected);
      }
    });
  }

  @Test
  public void assumeRoleRefusalLeavesAsTheRawStsExceptionWithEvidence()
      throws Throwable {
    Map<String, String> credentials =
        Map.of(KinesisConfig.ACCESS_KEY, FAKE_ACCESS_KEY, KinesisConfig.SECRET_KEY, FAKE_SECRET_KEY,
            KinesisConfig.IAM_ROLE_BASED_ACCESS_ENABLED, "true", KinesisConfig.ROLE_ARN, FAKE_ROLE_ARN);
    withSystemProperties(Map.of(STS_ENDPOINT_PROPERTY, _endpoint), () -> {
      try (KinesisConsumer consumer = new KinesisConsumer(new KinesisConfig(streamConfig(credentials)))) {
        // STS rejects the table's base keys. The credential cache fails each refresh with a new exception, so the
        // evidence can go on the SDK's own instance.
        _stsErrorCode = "InvalidClientTokenId";
        StsException rejected =
            expectThrows(StsException.class, () -> consumer.fetchMessages(START_OFFSET, TIMEOUT_MS));
        assertAuth(rejected, IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_STS);
        assertNotSame(expectThrows(StsException.class, () -> consumer.fetchMessages(START_OFFSET, TIMEOUT_MS)),
            rejected);

        // The table's role refuses the assumption
        _stsErrorCode = "AccessDenied";
        StsException denied =
            expectThrows(StsException.class, () -> consumer.fetchMessages(START_OFFSET, TIMEOUT_MS));
        assertEquals(IngestionSourceErrorEvidence.of(denied).getCredentialMode(),
            IngestionSourceCredentialMode.STATIC);
        assertAuth(denied, IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_STS);
      }
    });
    // No credential, so no request ever reached Kinesis
    assertEquals(_stsRequests.get(), 3);
    assertEquals(_kinesisRequests.get(), 0);
  }

  private static void withSystemProperties(Map<String, String> properties, ThrowingRunnable body)
      throws Throwable {
    Map<String, String> previous = new HashMap<>();
    properties.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));
    try {
      body.run();
    } finally {
      previous.forEach((key, value) -> {
        if (value != null) {
          System.setProperty(key, value);
        } else {
          System.clearProperty(key);
        }
      });
    }
  }
}
