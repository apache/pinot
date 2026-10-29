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
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.SSLHandshakeException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_BROKER;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_IDP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Pins the classifier's Kafka broker rules against the kafka-clients version on the test classpath: 3.x here, and
/// 4.x through the subclass in pinot-kafka-4.0, where the authentication classes moved under
/// `InvalidConfigurationException` and the OAuth login reports failures differently. Where it is cheap, a fixture is
/// produced by the real client.
public class KafkaSourceErrorClassificationTest {

  /// The consume loop sees the broker's exception raw, under Pinot's own wrappers.
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", failure);
  }

  @Test
  public void brokerRejectsTheCredential() {
    // A broker rejection carries no cause.
    assertAuth(underPinot(new SaslAuthenticationException(
            "Authentication failed during authentication due to invalid credentials with SASL mechanism PLAIN")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);
    assertAuth(new UnsupportedSaslMechanismException("Client SASL mechanism 'PLAIN' not enabled in the server"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);
    assertAuth(new IllegalSaslStateException("Unexpected handshake request"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);
    // MSK IAM reports a policy denial the same way, whichever identity signed the request.
    assertAuth(underPinot(new SaslAuthenticationException("[uuid]: Access denied")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);
  }

  @Test
  public void clientSideSaslFailureIsAuthWithNoTarget() {
    // The MSK IAM login module failing to load the process's credentials, as kafka-clients and aws-msk-iam-auth
    // build it.
    Throwable credentialsGone = new SaslAuthenticationException(
        "An error: (java.security.PrivilegedActionException: javax.security.sasl.SaslException: Failed to find AWS "
            + "IAM Credentials [Caused by software.amazon.awssdk.core.exception.SdkClientException: Unable to load "
            + "credentials from any of the providers in the chain]) occurred when evaluating SASL token received from "
            + "the Kafka Broker.",
        new SaslException("Failed to find AWS IAM Credentials", SdkClientException.create(
            "Unable to load credentials from any of the providers in the chain AwsCredentialsProviderChain")));
    assertAuth(underPinot(credentialsGone), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // Any client-side failure building the SASL token: authentication failed before the broker judged anything.
    Throwable kerberosClient = new SaslAuthenticationException("An error occurred when evaluating SASL token",
        new SaslException("GSS initiate failed", new IllegalStateException("no valid credentials")));
    assertAuth(underPinot(kerberosClient), IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  @Test
  public void connectionThrottleIsSourceRead() {
    assertSourceRead(new SaslAuthenticationException("[uuid]: Too many connects"), TARGET_BROKER);
  }

  @Test
  public void topicAclDenied() {
    assertAuth(underPinot(new TopicAuthorizationException(Set.of("events"))),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_BROKER);
  }

  @Test
  public void tlsFailureDependsOnWhichSideRejectedTheCertificate() {
    Throwable weRejectedTheBroker = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("PKIX path building failed",
            new CertificateException("unable to find valid certification path to requested target")));
    assertSourceRead(underPinot(weRejectedTheBroker), TARGET_BROKER);

    Throwable brokerRejectedUs = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("Received fatal alert: bad_certificate"));
    assertAuth(underPinot(brokerRejectedUs), IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);

    Throwable otherHandshakeFailure = new SslAuthenticationException("SSL handshake failed",
        new SSLHandshakeException("Received fatal alert: handshake_failure"));
    assertSourceRead(otherHandshakeFailure, TARGET_BROKER);
  }

  @Test
  public void brokerNeverAnswered() {
    assertSourceRead(underPinot(new TimeoutException("Timeout expired while fetching topic metadata")), TARGET_BROKER);
  }

  @Test
  public void unresolvableBootstrapFromTheRealClient() {
    Properties props = consumerProperties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "no-such-broker.invalid:9092");
    assertSourceRead(underPinot(constructConsumer(props)), TARGET_BROKER);
  }

  @Test
  public void clientConfigErrorIsSourceRead() {
    assertSourceRead(new KafkaException("Failed to construct kafka consumer",
        new ConfigException("Invalid value FOO for configuration security.protocol")), null);
    // In 4.x the authentication classes extend this one: it must never decide on its own.
    assertNotClassified(new InvalidConfigurationException("Invalid configuration"));
    // A keystore Pinot can't read, from the real client: no rule recognises it yet (a listed gap).
    Properties props = consumerProperties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put("security.protocol", "SSL");
    props.put("ssl.keystore.location", "/nonexistent/keystore.jks");
    props.put("ssl.keystore.password", "fixture-only");
    assertNotClassified(underPinot(constructConsumer(props)));
  }

  @Test
  public void expiredOffsetWithoutResetPolicyIsSourceRead() {
    assertSourceRead(underPinot(new OffsetOutOfRangeException("Fetch position out of range",
        Map.of(new TopicPartition("events", 0), 42L))), TARGET_BROKER);
  }

  @Test
  public void oauthTokenEndpointRefusesTheClientInTheRealClient()
      throws Exception {
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(401, "{\"error\":\"invalid_client\"}")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP).getCode(), "invalid_client");
    // RFC 6749 refusals arrive as 400.
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(400, "{\"error\":\"invalid_grant\"}")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP).getCode(), "invalid_grant");
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(400, "{\"error\":\"unauthorized_client\"}")),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_IDP).getCode(), "unauthorized_client");
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(403, "{}")), IngestionSourceErrorSubReason.AUTHORIZATION,
        TARGET_IDP).getCode(), "403");
    // The endpoint refusing the token request itself, whatever the status.
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(400, "{\"error\":\"invalid_request\"}")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP).getCode(), "invalid_request");
    assertEquals(assertAuth(underPinot(constructOAuthConsumer(401, "{\"error\":\"invalid_request\"}")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP).getCode(), "invalid_request");
  }

  @Test
  public void oauthTokenEndpointThatFailedIsSourceReadInTheRealClient()
      throws Exception {
    assertSourceRead(underPinot(constructOAuthConsumer(503, "unavailable")), TARGET_IDP);
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    assertSourceRead(underPinot(constructOAuthConsumer("http://localhost:" + closedPort + "/token")), TARGET_IDP);
  }

  @Test
  public void kerberosLogin() {
    Throwable rejected = new KafkaException("Failed to construct kafka consumer",
        new KafkaException(new LoginException("Pre-authentication information was invalid (24)")));
    assertAuth(rejected, IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
    assertAuth(new LoginException("Client not found in Kerberos database (6)"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
    assertSourceRead(new LoginException("Cannot contact any KDC for realm 'EXAMPLE.COM'"), TARGET_IDP);
    assertSourceRead(new LoginException("Receive timed out"), TARGET_IDP);
    // Our clock, keytab or krb5 setup: the login failed without the KDC rejecting the principal.
    assertAuth(new LoginException("Unable to obtain password from user"), IngestionSourceErrorSubReason.AUTHENTICATION,
        null);
    assertAuth(new LoginException("Clock skew too great (37)"), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(new LoginException("Cannot locate default realm"), IngestionSourceErrorSubReason.AUTHENTICATION, null);
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
      assertAuth(underPinot(denied), IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_BROKER);
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
    try {
      return constructOAuthConsumer("http://localhost:" + server.getAddress().getPort() + "/token");
    } finally {
      server.stop(0);
    }
  }

  private static Throwable constructOAuthConsumer(String tokenUrl) {
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
    }
  }
}
