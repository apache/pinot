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
package org.apache.pinot.plugin.stream.kafka;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.SSLHandshakeException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pinot.spi.ingestion.error.IngestionSourceCredentialMode;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Pins the classifier's Kafka broker rules against the kafka-clients version on the test classpath: 3.x here, and
/// 4.x through the subclass in pinot-kafka-4.0, where the authentication classes moved under
/// `InvalidConfigurationException`. Where it is cheap, a fixture is produced by the real client.
public class KafkaSourceErrorClassificationTest {
  private static final String BROKER = "broker";
  private static final String IDP = "idp";

  private static void assertAuth(Throwable failure, IngestionSourceErrorSubReason subReason, String target) {
    IngestionSourceErrorClassification classification = classify(failure);
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.AUTH, describe(failure));
    assertEquals(classification.getSubReason(), subReason, describe(failure));
    assertEquals(classification.getTarget(), target, describe(failure));
  }

  private static void assertSourceRead(Throwable failure, String target) {
    IngestionSourceErrorClassification classification = classify(failure);
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.SOURCE_READ, describe(failure));
    assertEquals(classification.getTarget(), target, describe(failure));
  }

  private static void assertNotClassified(Throwable failure) {
    Optional<IngestionSourceErrorClassification> classification = IngestionSourceErrorClassifier.classify(failure);
    assertTrue(classification.isEmpty(), classification + " for " + describe(failure));
  }

  private static IngestionSourceErrorClassification classify(Throwable failure) {
    return IngestionSourceErrorClassifier.classify(failure)
        .orElseThrow(() -> new AssertionError("Not classified: " + describe(failure)));
  }

  private static String describe(Throwable failure) {
    return ExceptionUtils.getStackTrace(failure);
  }

  /// The consume loop sees the broker's exception raw, under Pinot's own wrappers.
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", failure);
  }

  @Test
  public void brokerRejectsTheCredential() {
    // A broker rejection carries no cause.
    assertAuth(underPinot(new SaslAuthenticationException(
            "Authentication failed during authentication due to invalid credentials with SASL mechanism PLAIN")),
        IngestionSourceErrorSubReason.AUTHENTICATION, BROKER);
  }

  @Test
  public void cloudIdentityDenialIsUnspecified() {
    // MSK IAM reports a policy denial as a SASL authentication failure; the plugin records it signs with a cloud
    // identity, so the class can't say which.
    Throwable failure = IngestionSourceErrorEvidence.attach(new SaslAuthenticationException("[uuid]: Access denied"),
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.AMBIENT)
            .setRoleArnConfigured(true));
    assertAuth(underPinot(failure), IngestionSourceErrorSubReason.UNSPECIFIED, BROKER);
  }

  @Test
  public void clientSideSaslFailureIsNotTheBrokers() {
    // Our credential chain or Kerberos setup failed while building the SASL token: the broker judged nothing.
    Throwable ours = new SaslAuthenticationException(
        "An error: (java.security.PrivilegedActionException: javax.security.sasl.SaslException: Failed to find AWS "
            + "IAM Credentials) occurred when evaluating SASL token received from the Kafka Broker.",
        new SaslException("Failed to find AWS IAM Credentials", new IllegalStateException("no credentials")));
    assertNotClassified(underPinot(ours));
  }

  @Test
  public void connectionThrottleIsSourceRead() {
    assertSourceRead(new SaslAuthenticationException("[uuid]: Too many connects"), BROKER);
  }

  @Test
  public void topicAclDenied() {
    assertAuth(underPinot(new TopicAuthorizationException(Set.of("events"))),
        IngestionSourceErrorSubReason.AUTHORIZATION, BROKER);
  }

  @Test
  public void tlsFailureDependsOnWhichSideRejectedTheCertificate() {
    Throwable weRejectedTheBroker = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("PKIX path building failed",
            new CertificateException("unable to find valid certification path to requested target")));
    assertSourceRead(underPinot(weRejectedTheBroker), BROKER);

    Throwable brokerRejectedUs = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("Received fatal alert: bad_certificate"));
    assertAuth(underPinot(brokerRejectedUs), IngestionSourceErrorSubReason.AUTHENTICATION, BROKER);

    Throwable expiredClientCertificate = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("Received fatal alert: certificate_expired"));
    assertAuth(expiredClientCertificate, IngestionSourceErrorSubReason.AUTHENTICATION, BROKER);

    Throwable otherHandshakeFailure = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("Received fatal alert: handshake_failure"));
    assertSourceRead(otherHandshakeFailure, BROKER);
  }

  @Test
  public void brokerNeverAnswered() {
    assertSourceRead(underPinot(new TimeoutException("Timeout expired while fetching topic metadata")), BROKER);
  }

  @Test
  public void unresolvableBootstrapFromTheRealClient() {
    Properties props = consumerProperties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "no-such-broker.invalid:9092");
    assertSourceRead(underPinot(constructConsumer(props)), BROKER);
  }

  @Test
  public void ourConfigIsNotTheSource() {
    assertNotClassified(new KafkaException("Failed to construct kafka consumer",
        new ConfigException("Invalid value FOO for configuration security.protocol")));
    // In 4.x the authentication classes extend this one: it must never decide on its own.
    assertNotClassified(new InvalidConfigurationException("Invalid configuration"));
  }

  @Test
  public void expiredOffsetWithoutResetPolicyIsSourceRead() {
    assertSourceRead(underPinot(new OffsetOutOfRangeException("Fetch position out of range",
        Map.of(new TopicPartition("events", 0), 42L))), BROKER);
  }

  @Test
  public void oauthTokenEndpointRefusesTheClientInTheRealClient()
      throws Exception {
    assertAuth(underPinot(constructOAuthConsumer(401, "{\"error\":\"invalid_client\"}")),
        IngestionSourceErrorSubReason.AUTHENTICATION, IDP);
    assertAuth(underPinot(constructOAuthConsumer(403, "{}")), IngestionSourceErrorSubReason.AUTHORIZATION, IDP);
    // A malformed token request is ours.
    assertNotClassified(underPinot(constructOAuthConsumer(400, "{\"error\":\"invalid_request\"}")));
  }

  @Test
  public void kerberosLogin() {
    Throwable rejected = new KafkaException("Failed to construct kafka consumer",
        new KafkaException(new LoginException("Pre-authentication information was invalid (24)")));
    assertAuth(rejected, IngestionSourceErrorSubReason.AUTHENTICATION, IDP);
    assertSourceRead(new LoginException("Cannot contact any KDC for realm 'EXAMPLE.COM'"), IDP);
    assertNotClassified(new LoginException("Unable to obtain password from user"));
  }

  @Test
  public void classifiesClassesFromAnotherClassLoader()
      throws Exception {
    // Plugins load kafka-clients in their own classloader: the classifier must match by name, never by instanceof.
    URL kafkaClients = TopicAuthorizationException.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader isolated = new URLClassLoader(new URL[]{kafkaClients}, ClassLoader.getPlatformClassLoader())) {
      Class<?> isolatedClass = isolated.loadClass(TopicAuthorizationException.class.getName());
      assertTrue(isolatedClass != TopicAuthorizationException.class);
      Throwable denied = (Throwable) isolatedClass.getConstructor(String.class).newInstance("denied");
      assertAuth(underPinot(denied), IngestionSourceErrorSubReason.AUTHORIZATION, BROKER);
    }
  }

  private static Properties consumerProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }

  private static Throwable constructConsumer(Properties props) {
    try (KafkaConsumer<byte[], byte[]> ignored = new KafkaConsumer<>(props)) {
      fail("Expected the consumer construction to fail");
      return null;
    } catch (KafkaException e) {
      return e;
    }
  }

  /// Builds an OAUTHBEARER consumer whose token endpoint answers `status` with `body`. The login runs in the
  /// constructor, before any broker connection.
  private static Throwable constructOAuthConsumer(int status, String body)
      throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/token", exchange -> {
      byte[] response = body.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    });
    server.start();
    String tokenUrl = "http://localhost:" + server.getAddress().getPort() + "/token";
    String allowedUrls = System.getProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls");
    System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", tokenUrl);
    try {
      Properties props = consumerProperties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put("security.protocol", "SASL_PLAINTEXT");
      props.put("sasl.mechanism", "OAUTHBEARER");
      props.put("sasl.oauthbearer.token.endpoint.url", tokenUrl);
      props.put("sasl.login.callback.handler.class",
          "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
      props.put("sasl.login.retry.backoff.ms", "1");
      props.put("sasl.login.retry.backoff.max.ms", "1");
      props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
          + "clientId=\"fixture-client\" clientSecret=\"fixture-only\";");
      return constructConsumer(props);
    } finally {
      if (allowedUrls != null) {
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", allowedUrls);
      } else {
        System.clearProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls");
      }
      server.stop(0);
    }
  }
}
