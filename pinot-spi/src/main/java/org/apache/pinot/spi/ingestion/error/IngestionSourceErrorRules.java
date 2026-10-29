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
package org.apache.pinot.spi.ingestion.error;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;


/// The built-in rules [IngestionSourceErrorClassifier] applies, organised by SDK.
///
/// Rules key on the source's own class and code first, and on an HTTP status only where the source has no code of
/// its own (a schema registry, Pulsar's admin API, an OAuth token endpoint) or AWS returned none. They read a message
/// only where the client stringified its evidence or the class covers several failures; each such match
/// is commented at its rule and pinned by a fixture test against the SDK version Pinot ships. A phrase an SDK upgrade
/// changes can move a failure to another category or leave it unclassified, so an upgrade of these SDKs reruns those
/// fixtures against real clients where they can.
///
/// They never key on Pinot's own wrappers, an outer plugin message, a bare Kafka `SerializationException` or
/// `InvalidConfigurationException`, or which exceptions a client considers retriable.
///
/// A rule returns a verdict to decide its node, [IngestionSourceErrorVerdict#none] when the failure is not a failure to
/// access the source, or null to leave the node undecided. No rule asks whose credential failed or who can fix it:
/// Pinot's own identity, clock and setup read like the table's.
final class IngestionSourceErrorRules {
  private IngestionSourceErrorRules() {
  }

  private static final String TARGET_ADMIN = IngestionSourceErrorVerdict.TARGET_ADMIN;
  private static final String TARGET_BROKER = IngestionSourceErrorVerdict.TARGET_BROKER;
  private static final String TARGET_IDP = IngestionSourceErrorVerdict.TARGET_IDP;
  private static final String TARGET_KMS = IngestionSourceErrorVerdict.TARGET_KMS;
  private static final String TARGET_SCHEMA_REGISTRY = IngestionSourceErrorVerdict.TARGET_SCHEMA_REGISTRY;
  private static final String TARGET_STS = IngestionSourceErrorVerdict.TARGET_STS;

  // Our own interrupts and cancellations, matched exactly: SocketTimeoutException extends InterruptedIOException.
  private static final Set<String> INTERRUPTS = Set.of("java.lang.InterruptedException",
      "java.io.InterruptedIOException", "java.nio.channels.ClosedByInterruptException",
      "java.util.concurrent.CancellationException");

  // ---------------------------------------------------------------------------------------------------------------
  // Evidence recorded by a client that is about to discard it

  @Nullable
  static IngestionSourceErrorVerdict matchEvidence(IngestionSourceErrorEvidence evidence) {
    String code = evidence.getCode();
    if (IngestionSourceErrorEvidence.UNREACHABLE.equals(code)) {
      return sourceRead(code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.LOCAL_PRECHECK.equals(code)) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION.equals(code)) {
      return sourceRead(code, evidence.getTarget() != null ? evidence.getTarget() : TARGET_BROKER);
    }
    // A status or another code means something only to the source that recorded it: a registered rule reads it.
    return null;
  }

  /// Source-specific rules for one node, reading the part of its chain beneath it where a rule needs to.
  @Nullable
  static IngestionSourceErrorVerdict matchSource(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
    Class<?> clazz = node.getClass();
    if (INTERRUPTS.contains(clazz.getName())) {
      return none(clazz.getSimpleName());
    }
    if (hasClass(clazz, AWS_SERVICE_EXCEPTION)) {
      return awsService(node, evidence);
    }
    if (hasClass(clazz, AWS_CLIENT_EXCEPTION)) {
      return awsClient(node, below);
    }
    if (hasClass(clazz, CONFLUENT_REST_CLIENT_EXCEPTION)) {
      return schemaRegistry(node);
    }
    IngestionSourceErrorVerdict kafka = kafka(node, below);
    if (kafka != null) {
      return kafka;
    }
    if (hasClass(clazz, LOGIN_EXCEPTION)) {
      return login(node);
    }
    if (hasClass(clazz, PULSAR_CLIENT_EXCEPTION) || hasClass(clazz, PULSAR_V5_CLIENT_EXCEPTION)) {
      return pulsar(node);
    }
    if (hasClass(clazz, PULSAR_ADMIN_EXCEPTION)) {
      return pulsarAdmin(node);
    }
    if (hasClass(clazz, GAX_API_EXCEPTION)) {
      Object statusCode = invoke(node, "getStatusCode");
      return grpc(statusCode != null ? invoke(statusCode, "getCode") : null, node, below);
    }
    if (hasClass(clazz, GRPC_STATUS_RUNTIME_EXCEPTION) || hasClass(clazz, GRPC_STATUS_EXCEPTION)) {
      Object status = invoke(node, "getStatus");
      return grpc(status != null ? invoke(status, "getCode") : null, node, below);
    }
    if (hasClass(clazz, GOOGLE_AUTH_EXCEPTION)) {
      return googleAuth(below);
    }
    return null;
  }

  /// Generic transport rules, applied only when no source-specific rule decided the chain.
  @Nullable
  static IngestionSourceErrorVerdict matchGeneric(Throwable node) {
    IngestionSourceErrorVerdict refused = tlsAlert(node, null);
    if (refused != null) {
      return refused;
    }
    return isNetworkFailure(node) ? sourceRead(node.getClass().getSimpleName(), null) : null;
  }

  /// A TLS peer's alert rejecting the certificate we presented, or denying us access, or null for any other failure.
  @Nullable
  private static IngestionSourceErrorVerdict tlsAlert(Throwable node, @Nullable String target) {
    if (!(node instanceof SSLException)) {
      return null;
    }
    Matcher alert = TLS_ALERT.matcher(messageOf(node));
    if (!alert.find()) {
      return null;
    }
    String name = alert.group(1);
    if (TLS_CERTIFICATE_REJECTED_ALERTS.contains(name)) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, name, target);
    }
    return TLS_ACCESS_DENIED_ALERT.equals(name)
        ? auth(IngestionSourceErrorSubReason.AUTHORIZATION, name, target)
        : null;
  }

  /// Whether `throwable` is a network failure: DNS, connect, TLS, a socket error or a timeout.
  private static boolean isNetworkFailure(Throwable throwable) {
    return throwable instanceof UnknownHostException || throwable instanceof SocketException
        || throwable instanceof SocketTimeoutException || throwable instanceof HttpTimeoutException
        || throwable instanceof SSLException;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // AWS SDK v2: Kinesis, STS, KMS, Glue

  private static final String AWS_SERVICE_EXCEPTION = "software.amazon.awssdk.awscore.exception.AwsServiceException";
  private static final String AWS_CLIENT_EXCEPTION = "software.amazon.awssdk.core.exception.SdkClientException";
  private static final String AWS_STS_EXCEPTION = "software.amazon.awssdk.services.sts.model.StsException";
  private static final String AWS_ABORTED = "software.amazon.awssdk.core.exception.AbortedException";
  // The SDK's Apache HTTP clients, 4 and 5: the endpoint closed the connection without answering
  private static final Set<String> APACHE_NO_HTTP_RESPONSE = Set.of("org.apache.http.NoHttpResponseException",
      "org.apache.hc.core5.http.NoHttpResponseException");
  private static final Set<String> AWS_CALL_TIMEOUTS = Set.of(
      "software.amazon.awssdk.core.exception.ApiCallTimeoutException",
      "software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException");
  // Codes that reject a credential, a web identity token, or a request's signature, our clock's skew included.
  private static final Set<String> AWS_CREDENTIAL_CODES = Set.of("InvalidAccessKeyId", "SignatureDoesNotMatch",
      "InvalidSignatureException", "ExpiredToken", "ExpiredTokenException", "InvalidToken", "TokenRefreshRequired",
      "InvalidClientTokenId", "IncompleteSignature", "MissingAuthenticationToken", "UnrecognizedClientException",
      "RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture", "InvalidIdentityToken", "IDPRejectedClaim");
  private static final Set<String> AWS_AUTHORIZATION_CODES = Set.of("RegionDisabledException",
      "GlueEncryptionException", "AccountProblem", "NotSignedUp");
  private static final Set<String> AWS_KMS_AUTHORIZATION_CODES = Set.of("KMSAccessDeniedException",
      "KMSOptInRequired", "KMSKeyNotAccessibleFault");
  private static final Set<String> AWS_SOURCE_READ_CODES = Set.of("NoSuchKey", "NoSuchBucket", "InvalidObjectState",
      "ObjectNotInActiveTierError", "PermanentRedirect", "AuthorizationHeaderMalformed",
      "IllegalLocationConstraintException", "SlowDown", "ServiceUnavailable", "ServiceUnavailableException",
      "InternalError", "InternalFailure", "Throttling", "ThrottlingException", "EntityNotFoundException",
      "InternalServiceException", "AllAccessDisabled", "InvalidPayer", "ResourceNotFoundException",
      "ResourceInUseException", "ProvisionedThroughputExceededException", "LimitExceededException",
      "ExpiredIteratorException", "ExpiredNextTokenException", "IDPCommunicationError");
  private static final Set<String> AWS_KMS_SOURCE_READ_CODES = Set.of("KMS.DisabledException",
      "KMS.KMSInvalidStateException", "KMS.NotFoundException", "KMSDisabledException", "KMSInvalidStateException",
      "KMSNotFoundException", "KMSThrottlingException");
  // A request the service rejected as invalid: STS refusing an AssumeRole request, or a source refusing a read.
  private static final Set<String> AWS_INVALID_REQUEST_CODES = Set.of("ValidationError", "ValidationException",
      "MalformedPolicyDocument", "PackedPolicyTooLarge", "InvalidInputException", "InvalidArgumentException");

  @Nullable
  private static IngestionSourceErrorVerdict awsService(Throwable node,
      @Nullable IngestionSourceErrorEvidence evidence) {
    boolean sts = hasClass(node.getClass(), AWS_STS_EXCEPTION);
    Object details = invoke(node, "awsErrorDetails");
    Object code = details != null ? invoke(details, "errorCode") : null;
    Object status = invoke(node, "statusCode");
    return awsCode(sts, code != null ? code.toString() : null, status instanceof Integer ? (Integer) status : 0,
        messageOf(node), evidence);
  }

  @Nullable
  private static IngestionSourceErrorVerdict awsCode(boolean sts, @Nullable String code, int status, String message,
      @Nullable IngestionSourceErrorEvidence evidence) {
    String target = sts ? TARGET_STS : evidence != null ? evidence.getTarget() : null;
    if (code == null || code.isEmpty()) {
      // A response with no body (a HEAD): a deny, a missing object without list permission and our skew look alike.
      if (status == 403) {
        return auth(IngestionSourceErrorSubReason.MISSING_OR_DENIED, "403", target);
      }
      if (status == 404 || status == 400 || status == 429 || (status >= 300 && status < 400) || status >= 500) {
        return sourceRead(String.valueOf(status), target);
      }
      return null;
    }
    if (code.equals("AccessDenied") || code.equals("AccessDeniedException")) {
      if (message.contains("ciphertext refers to a customer master key that does not exist")) {
        return auth(IngestionSourceErrorSubReason.MISSING_OR_DENIED, code, TARGET_KMS);
      }
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, message.contains("kms:") ? TARGET_KMS : target);
    }
    if (AWS_AUTHORIZATION_CODES.contains(code)) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, target);
    }
    if (AWS_KMS_AUTHORIZATION_CODES.contains(code)) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_KMS);
    }
    if (AWS_CREDENTIAL_CODES.contains(code)) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, target);
    }
    if (AWS_KMS_SOURCE_READ_CODES.contains(code)) {
      return sourceRead(code, TARGET_KMS);
    }
    if (AWS_SOURCE_READ_CODES.contains(code) || status == 429 || status >= 500) {
      return sourceRead(code, target);
    }
    if (AWS_INVALID_REQUEST_CODES.contains(code)) {
      return sts ? auth(IngestionSourceErrorSubReason.UNSPECIFIED, code, target) : sourceRead(code, target);
    }
    return null;
  }

  /// No service exception sits beneath this node, or it would have decided first.
  @Nullable
  private static IngestionSourceErrorVerdict awsClient(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String message = messageOf(node);
    if (hasClass(clazz, AWS_ABORTED)) {
      // Our cancellation.
      return none(clazz.getSimpleName());
    }
    // A credential chain that found no credential ("Unable to load credentials from any of the providers ..."), or one
    // of its providers called directly after a first success ("Failed to load credentials from IMDS." or "... from
    // metadata service.").
    if (message.startsWith("Unable to load credentials") || message.startsWith("Failed to load credentials from")) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, clazz.getSimpleName(), null);
    }
    if (hasAnyClass(clazz, AWS_CALL_TIMEOUTS)) {
      return sourceRead(clazz.getSimpleName(), null);
    }
    for (Throwable cause : below) {
      IngestionSourceErrorVerdict transport = matchGeneric(cause);
      if (transport != null) {
        return transport;
      }
      if (hasAnyClass(cause.getClass(), APACHE_NO_HTTP_RESPONSE)) {
        return sourceRead(cause.getClass().getSimpleName(), null);
      }
    }
    // Includes the cause-less "Request attempt N failure" records the SDK attaches as suppressed exceptions.
    return null;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Kafka brokers (kafka-clients 3.x and 4.x)

  private static final String KAFKA_ERRORS = "org.apache.kafka.common.errors.";
  private static final String KAFKA_AUTHENTICATION = KAFKA_ERRORS + "AuthenticationException";
  private static final String KAFKA_AUTHORIZATION = KAFKA_ERRORS + "AuthorizationException";
  private static final String KAFKA_SASL_AUTHENTICATION = KAFKA_ERRORS + "SaslAuthenticationException";
  private static final String KAFKA_SSL_AUTHENTICATION = KAFKA_ERRORS + "SslAuthenticationException";
  private static final String KAFKA_CONFIG_EXCEPTION = "org.apache.kafka.common.config.ConfigException";
  private static final Set<String> KAFKA_BROKER_SOURCE_READ = Set.of(KAFKA_ERRORS + "TimeoutException",
      KAFKA_ERRORS + "DisconnectException", KAFKA_ERRORS + "UnknownTopicOrPartitionException",
      KAFKA_ERRORS + "OffsetOutOfRangeException", "org.apache.kafka.clients.consumer.OffsetOutOfRangeException");
  // The schema-registry serde maps a 429 to this class and drops the cause.
  private static final String KAFKA_THROTTLING_QUOTA = KAFKA_ERRORS + "ThrottlingQuotaExceededException";
  private static final String SASL_EXCEPTION = "javax.security.sasl.SaslException";
  // A certificate we failed to validate: we rejected the broker.
  private static final Set<String> CERTIFICATE_REJECTED = Set.of("java.security.cert.CertificateException",
      "java.security.cert.CertPathValidatorException", "sun.security.validator.ValidatorException");
  private static final Pattern TLS_ALERT = Pattern.compile("Received fatal alert: ([a-z_]+)");
  // Alerts a peer sends when it rejects the certificate we presented.
  private static final Set<String> TLS_CERTIFICATE_REJECTED_ALERTS = Set.of("bad_certificate",
      "unsupported_certificate", "certificate_revoked", "certificate_expired", "certificate_unknown", "unknown_ca",
      "certificate_required");
  private static final String TLS_ACCESS_DENIED_ALERT = "access_denied";

  @Nullable
  private static IngestionSourceErrorVerdict kafka(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String code = clazz.getSimpleName();
    if (hasClass(clazz, KAFKA_AUTHORIZATION)) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_BROKER);
    }
    if (hasClass(clazz, KAFKA_AUTHENTICATION)) {
      return kafkaAuthentication(node, below);
    }
    if (hasClass(clazz, KAFKA_THROTTLING_QUOTA)) {
      return sourceRead(code, TARGET_SCHEMA_REGISTRY);
    }
    if (hasAnyClass(clazz, KAFKA_BROKER_SOURCE_READ)) {
      // A broker's timeout carries no cause. The schema-registry serde reuses these classes over the network cause of
      // its own request, so with a cause the endpoint is unknown here.
      return sourceRead(code, below.isEmpty() ? TARGET_BROKER : null);
    }
    if (hasClass(clazz, KAFKA_CONFIG_EXCEPTION)) {
      // An unresolvable bootstrap list names the broker; any other config error is the client's own.
      return sourceRead(code, messageOf(node).startsWith("No resolvable bootstrap urls") ? TARGET_BROKER : null);
    }
    return null;
  }

  /// Kafka's authentication classes say less than they seem to. A client-side failure to build the SASL token (a
  /// credential chain or Kerberos setup) arrives under the same class with a `SaslException` cause, a broker's
  /// connection throttle reuses it, and an SSL failure covers both directions of a rejected certificate. MSK IAM
  /// reports a policy denial as an authentication failure too, so the sub-reason follows Kafka's class.
  private static IngestionSourceErrorVerdict kafkaAuthentication(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String code = clazz.getSimpleName();
    if (hasClass(clazz, KAFKA_SASL_AUTHENTICATION) && messageOf(node).contains("Too many connects")) {
      return sourceRead("too_many_connects", TARGET_BROKER);
    }
    if (hasClass(clazz, KAFKA_SSL_AUTHENTICATION)) {
      return kafkaTls(code, below);
    }
    for (Throwable cause : below) {
      if (hasClass(cause.getClass(), SASL_EXCEPTION)) {
        // A broker rejection carries no cause; this one failed in the client before the broker judged anything.
        return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, null);
      }
    }
    return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_BROKER);
  }

  private static IngestionSourceErrorVerdict kafkaTls(String code, List<Throwable> below) {
    for (Throwable cause : below) {
      if (hasAnyClass(cause.getClass(), CERTIFICATE_REJECTED)) {
        // We could not verify the broker: the endpoint, not our credential.
        return sourceRead(code, TARGET_BROKER);
      }
    }
    for (Throwable cause : below) {
      IngestionSourceErrorVerdict refused = tlsAlert(cause, TARGET_BROKER);
      if (refused != null) {
        return refused;
      }
    }
    // Any other handshake failure: the TLS connection to the endpoint failed.
    return sourceRead(code, TARGET_BROKER);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Confluent schema registry (kafka-schema-registry-client)

  private static final String CONFLUENT_REST_CLIENT_EXCEPTION =
      "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException";

  /// The registry's HTTP status decides; its error code does not (an HTML 403 from a proxy reads as 50005). The serde
  /// wraps this exception in the same Kafka authentication and authorization classes a broker uses, so it decides
  /// first, as the deeper node, and names the registry as the target.
  @Nullable
  private static IngestionSourceErrorVerdict schemaRegistry(Throwable node) {
    Object statusValue = invoke(node, "getStatus");
    if (!(statusValue instanceof Integer)) {
      return null;
    }
    int status = (Integer) statusValue;
    IngestionSourceErrorVerdict verdict = httpStatus(status, TARGET_SCHEMA_REGISTRY);
    // Any other status: a request the registry rejected.
    return verdict != null ? verdict : sourceRead(String.valueOf(status), TARGET_SCHEMA_REGISTRY);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Logins: Kerberos (Krb5LoginModule) and the Kafka OAUTHBEARER login, which both stringify their evidence

  private static final String LOGIN_EXCEPTION = "javax.security.auth.login.LoginException";
  // kafka-clients' token retriever, stringified into the LoginException: "The response code 401 and error response
  // {"invalid_client" - "..."} was encountered ..." for a refusal, "The unexpected response code 503 ..." (4.x) for a
  // failure.
  private static final Pattern OAUTH_RESPONSE_CODE = Pattern.compile("The (?:unexpected )?response code (\\d{3})");
  private static final Pattern OAUTH_ERROR =
      Pattern.compile("(?:\"error\"\\s*:\\s*\"|error response \\{\")([a-z_]{1,64})\"");
  // The 3.x token retriever's text when the endpoint failed or never answered.
  private static final String OAUTH_TOKEN_UNAVAILABLE = "An internal error occurred while retrieving token";
  // 4.x dumps the retriever's stack trace into the message instead: a network failure names its class there.
  private static final List<String> DUMPED_UNREACHABLE = List.of("java.net.ConnectException",
      "java.net.UnknownHostException", "java.net.SocketTimeoutException", "java.net.NoRouteToHostException",
      "java.net.http.HttpTimeoutException", "javax.net.ssl.SSLHandshakeException");
  // invalid_request: the endpoint refused the token request itself.
  private static final Set<String> OAUTH_AUTHENTICATION = Set.of("invalid_client", "invalid_grant",
      "unsupported_grant_type", "invalid_request");
  private static final Set<String> OAUTH_AUTHORIZATION = Set.of("unauthorized_client", "invalid_scope",
      "access_denied");
  private static final Set<String> OAUTH_UNAVAILABLE = Set.of("server_error", "temporarily_unavailable");
  private static final List<String> KERBEROS_UNREACHABLE = List.of("Cannot contact any KDC", "Receive timed out");
  // Our clock, keytab or krb5 setup: the login fails without the KDC rejecting the principal.
  private static final List<String> KERBEROS_LOCAL_FAILURES = List.of("Clock skew too great",
      "Unable to obtain password", "Cannot locate default realm", "Cannot locate KDC");
  // The KDC rejecting the principal: client not found (6), credentials revoked (18), pre-authentication failed (24).
  private static final Pattern KERBEROS_REJECTED = Pattern.compile("\\((6|18|24)\\)");

  @Nullable
  private static IngestionSourceErrorVerdict login(Throwable node) {
    String message = messageOf(node);
    Matcher responseCode = OAUTH_RESPONSE_CODE.matcher(message);
    if (responseCode.find()) {
      Matcher error = OAUTH_ERROR.matcher(message);
      return oauth(error.find() ? error.group(1) : null, Integer.parseInt(responseCode.group(1)));
    }
    if (message.contains(OAUTH_TOKEN_UNAVAILABLE)) {
      // The token endpoint failed or never answered; the client drops which.
      return sourceRead("token_unavailable", TARGET_IDP);
    }
    for (String unreachable : DUMPED_UNREACHABLE) {
      if (message.contains(unreachable)) {
        return sourceRead("token_unavailable", TARGET_IDP);
      }
    }
    for (String unreachable : KERBEROS_UNREACHABLE) {
      if (message.contains(unreachable)) {
        return sourceRead("kdc_unreachable", TARGET_IDP);
      }
    }
    for (String local : KERBEROS_LOCAL_FAILURES) {
      if (message.contains(local)) {
        return auth(IngestionSourceErrorSubReason.AUTHENTICATION, node.getClass().getSimpleName(), null);
      }
    }
    Matcher rejected = KERBEROS_REJECTED.matcher(message);
    if (rejected.find()) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, "krb5_" + rejected.group(1), TARGET_IDP);
    }
    return null;
  }

  /// An OAuth 2.0 token endpoint's `error` (RFC 6749 section 5.2), falling back to its status.
  @Nullable
  private static IngestionSourceErrorVerdict oauth(@Nullable String error, int status) {
    if (error != null) {
      String normalized = error.toLowerCase(Locale.ROOT);
      if (OAUTH_AUTHENTICATION.contains(normalized)) {
        return auth(IngestionSourceErrorSubReason.AUTHENTICATION, normalized, TARGET_IDP);
      }
      if (OAUTH_AUTHORIZATION.contains(normalized)) {
        return auth(IngestionSourceErrorSubReason.AUTHORIZATION, normalized, TARGET_IDP);
      }
      if (OAUTH_UNAVAILABLE.contains(normalized)) {
        return sourceRead(normalized, TARGET_IDP);
      }
    }
    return httpStatus(status, TARGET_IDP);
  }

  /// An HTTP status from a service with no error code of our own to read: 401 and 403 refuse us, 404, 408, 429 and
  /// 5xx can't serve us, anything else decides nothing.
  @Nullable
  private static IngestionSourceErrorVerdict httpStatus(int status, String target) {
    String code = String.valueOf(status);
    if (status == 401) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, target);
    }
    if (status == 403) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, target);
    }
    if (status == 404 || isUnavailable(status)) {
      return sourceRead(code, target);
    }
    return null;
  }

  /// Whether an HTTP status means the server couldn't serve the request now: a timeout, a throttle or a server error.
  private static boolean isUnavailable(int status) {
    return status == 408 || status == 429 || status >= 500;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Pulsar client (v4 and v5 APIs) and admin

  private static final String PULSAR_CLIENT_EXCEPTION = "org.apache.pulsar.client.api.PulsarClientException";
  private static final String PULSAR_V5_CLIENT_EXCEPTION = "org.apache.pulsar.client.api.v5.PulsarClientException";
  private static final String PULSAR_ADMIN_EXCEPTION = "org.apache.pulsar.client.admin.PulsarAdminException";
  private static final Set<String> PULSAR_SOURCE_READ = Set.of("TopicDoesNotExistException", "NotFoundException",
      "SubscriptionNotFoundException", "LookupException", "ConnectException", "TimeoutException",
      "BrokerMetadataException", "BrokerPersistenceException", "TopicTerminatedException", "NotAllowedException",
      "ConsumerBusyException", "NotConnectedException");
  // The client's own failures before or beside the broker: its config, its message checks
  private static final Set<String> PULSAR_CLIENT_SOURCE_READ = Set.of("InvalidServiceURL",
      "InvalidServiceURLException", "InvalidConfigurationException", "InvalidTopicNameException",
      "NotSupportedException", "CryptoException", "ChecksumException");
  // The client couldn't build its authentication data
  private static final Set<String> PULSAR_AUTHENTICATION_SETUP = Set.of("UnsupportedAuthenticationException",
      "GettingAuthenticationDataException");
  // The OAuth2 plugin's token errors: "Unable to obtain an access token: <description> (<error>)", or for a status
  // other than 400 or 401, "Unable to obtain an access token: Failed to perform HTTP request. res: <status>".
  private static final String PULSAR_OAUTH_TOKEN = "Unable to obtain an access token";
  private static final Pattern PULSAR_OAUTH_ERROR = Pattern.compile("\\(([a-z_]{1,64})\\)\\s*$");
  private static final Pattern PULSAR_OAUTH_STATUS = Pattern.compile("res: (\\d{3})\\s*$");

  /// Pulsar's class is its code: the broker's error maps one to one onto a nested subclass. Every subclass of the
  /// base class is matched on its own name and the names of its superclasses up to the base, so
  /// `ServiceNotReadyException` and `TooManyRequestsException` read as the `LookupException` they extend.
  @Nullable
  private static IngestionSourceErrorVerdict pulsar(Throwable node) {
    String message = messageOf(node);
    for (Class<?> current = node.getClass(); current != null; current = current.getSuperclass()) {
      String name = current.getName();
      if (name.equals(PULSAR_CLIENT_EXCEPTION) || name.equals(PULSAR_V5_CLIENT_EXCEPTION)) {
        // The base class alone: TLS failures surface only as "Connection already closed"; anything else, such as an
        // HTTP lookup failure, is left to a deeper or more specific exception.
        return message.contains("Connection already closed") ? sourceRead("connection_closed", TARGET_BROKER) : null;
      }
      String simple = name.substring(name.lastIndexOf('$') + 1);
      if (simple.equals("AuthenticationException")) {
        return pulsarAuthentication(message);
      }
      if (simple.equals("AuthorizationException")) {
        return auth(IngestionSourceErrorSubReason.AUTHORIZATION, simple, TARGET_BROKER);
      }
      if (PULSAR_SOURCE_READ.contains(simple)) {
        return sourceRead(simple, TARGET_BROKER);
      }
      if (PULSAR_CLIENT_SOURCE_READ.contains(simple)) {
        return sourceRead(simple, null);
      }
      if (PULSAR_AUTHENTICATION_SETUP.contains(simple)) {
        return auth(IngestionSourceErrorSubReason.AUTHENTICATION, simple, null);
      }
      if (simple.equals("AlreadyClosedException")) {
        // A client Pinot already closed.
        return none(simple);
      }
    }
    return null;
  }

  /// Pulsar's authentication class also covers an identity provider that failed or never answered, and a key file
  /// we couldn't read. Only the message tells them apart.
  private static IngestionSourceErrorVerdict pulsarAuthentication(String message) {
    String code = "AuthenticationException";
    if (message.startsWith("Unable to read private key")) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, null);
    }
    if (message.contains("Unable to retrieve OAuth 2.0 server metadata")) {
      return sourceRead(code, TARGET_IDP);
    }
    if (message.contains(PULSAR_OAUTH_TOKEN)) {
      Matcher error = PULSAR_OAUTH_ERROR.matcher(message);
      if (error.find()) {
        // An error we don't recognise decides nothing: it could be a refusal.
        return oauth(error.group(1), 0);
      }
      Matcher status = PULSAR_OAUTH_STATUS.matcher(message);
      if (status.find()) {
        IngestionSourceErrorVerdict verdict = httpStatus(Integer.parseInt(status.group(1)), TARGET_IDP);
        if (verdict != null) {
          return verdict;
        }
      }
      // Otherwise the token endpoint returned a status that decides nothing, or never answered.
      return sourceRead(code, TARGET_IDP);
    }
    // The broker rejected the token: it gives one generic message for every reason.
    return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_BROKER);
  }

  /// The admin client maps both 401 and 403 to `NotAuthorizedException`: only the status tells them apart. The base
  /// class without an HTTP error is a client-side failure that defaults its status to 500, so its status means nothing.
  @Nullable
  private static IngestionSourceErrorVerdict pulsarAdmin(Throwable node) {
    String simple = node.getClass().getSimpleName();
    if (simple.equals("ConnectException") || simple.equals("TimeoutException")) {
      return sourceRead(simple, TARGET_ADMIN);
    }
    if (PULSAR_AUTHENTICATION_SETUP.contains(simple)) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, simple, null);
    }
    if (node.getClass().getName().equals(PULSAR_ADMIN_EXCEPTION) && invoke(node, "getHttpError") == null) {
      return null;
    }
    Object status = invoke(node, "getStatusCode");
    return status instanceof Integer ? httpStatus((Integer) status, TARGET_ADMIN) : null;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // gRPC and gax (Google Cloud clients), and google-auth

  private static final String GAX_API_EXCEPTION = "com.google.api.gax.rpc.ApiException";
  private static final String GRPC_STATUS_RUNTIME_EXCEPTION = "io.grpc.StatusRuntimeException";
  private static final String GRPC_STATUS_EXCEPTION = "io.grpc.StatusException";
  private static final String GOOGLE_AUTH_EXCEPTION = "com.google.auth.oauth2.GoogleAuthException";
  private static final String GOOGLE_HTTP_RESPONSE_EXCEPTION = "com.google.api.client.http.HttpResponseException";
  private static final Set<String> GRPC_SOURCE_READ = Set.of("FAILED_PRECONDITION", "RESOURCE_EXHAUSTED",
      "DEADLINE_EXCEEDED", "INTERNAL", "INVALID_ARGUMENT", "ALREADY_EXISTS");
  // Google Cloud's deliberate conflation of a missing project with a denied one.
  private static final String GOOGLE_MISSING_OR_DENIED = "not found or user does not have access";

  @Nullable
  private static IngestionSourceErrorVerdict grpc(@Nullable Object codeValue, Throwable node, List<Throwable> below) {
    if (!(codeValue instanceof Enum)) {
      return null;
    }
    // Both gax's StatusCode.Code and gRPC's Status.Code are enums: read the constant's name, never a toString().
    String code = ((Enum<?>) codeValue).name();
    switch (code) {
      case "UNAUTHENTICATED":
        // The service's refusal, or grpc-auth's report of a token refresh that failed
        return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, null);
      case "PERMISSION_DENIED":
        return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, null);
      case "NOT_FOUND":
        if (messageOf(node).contains(GOOGLE_MISSING_OR_DENIED)) {
          return auth(IngestionSourceErrorSubReason.MISSING_OR_DENIED, code, null);
        }
        return sourceRead(code, null);
      case "UNAVAILABLE":
        // The transport failed, unless the peer rejected the certificate we presented
        for (Throwable cause : below) {
          IngestionSourceErrorVerdict refused = tlsAlert(cause, null);
          if (refused != null) {
            return refused;
          }
        }
        return sourceRead(code, null);
      case "CANCELLED":
        // Our cancellation.
        return none(code);
      default:
        return GRPC_SOURCE_READ.contains(code) ? sourceRead(code, null) : null;
    }
  }

  /// A token refresh that failed. A token endpoint that failed couldn't serve us; otherwise the status the transport
  /// built above it decides.
  @Nullable
  private static IngestionSourceErrorVerdict googleAuth(List<Throwable> below) {
    // grpc-auth reports a failure google-auth won't retry as UNAUTHENTICATED, a 502 or 504 among them.
    for (Throwable cause : below) {
      if (hasClass(cause.getClass(), GOOGLE_HTTP_RESPONSE_EXCEPTION)) {
        Object status = invoke(cause, "getStatusCode");
        if (status instanceof Integer && isUnavailable((Integer) status)) {
          return sourceRead(String.valueOf(status), TARGET_IDP);
        }
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Shorthands

  private static IngestionSourceErrorVerdict auth(IngestionSourceErrorSubReason subReason, @Nullable String code,
      @Nullable String target) {
    return IngestionSourceErrorVerdict.auth(subReason, code, target);
  }

  private static IngestionSourceErrorVerdict sourceRead(@Nullable String code, @Nullable String target) {
    return IngestionSourceErrorVerdict.sourceRead(code, target);
  }

  private static IngestionSourceErrorVerdict none(@Nullable String code) {
    return IngestionSourceErrorVerdict.none(code);
  }

  private static boolean hasClass(Class<?> clazz, String className) {
    return classNames(clazz).contains(className);
  }

  private static boolean hasAnyClass(Class<?> clazz, Set<String> classNames) {
    return !Collections.disjoint(classNames(clazz), classNames);
  }

  // The names of a class, its superclasses and every interface they implement. Only strings are cached, so a plugin
  // classloader is never pinned.
  private static final ClassValue<Set<String>> CLASS_NAMES = new ClassValue<>() {
    @Override
    protected Set<String> computeValue(Class<?> type) {
      Set<String> names = new HashSet<>();
      collectNames(type, names);
      return Set.copyOf(names);
    }
  };

  private static void collectNames(@Nullable Class<?> clazz, Set<String> names) {
    for (Class<?> current = clazz; current != null; current = current.getSuperclass()) {
      if (!names.add(current.getName())) {
        continue;
      }
      for (Class<?> iface : current.getInterfaces()) {
        collectNames(iface, names);
      }
    }
  }

  /// The names [IngestionSourceErrorRule#hasClass] matches against.
  static Set<String> classNames(Class<?> clazz) {
    return CLASS_NAMES.get(clazz);
  }

  private static String messageOf(Throwable throwable) {
    return IngestionSourceErrorRule.messageOf(throwable);
  }

  @Nullable
  private static Object invoke(Object target, String methodName) {
    return IngestionSourceErrorRule.invoke(target, methodName);
  }
}
