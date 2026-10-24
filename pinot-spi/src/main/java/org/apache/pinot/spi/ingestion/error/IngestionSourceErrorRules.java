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
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;


/// The built-in rules [IngestionSourceErrorClassifier] applies, organised by SDK.
///
/// Rules key on the source's own class and code first. They read a message only in a few enumerated cases where the
/// client stringified its evidence, each pinned by a test against the SDK version Pinot ships: Kafka's OAuth and
/// Kerberos login failures and its "Too many connects" throttle, the peer's TLS alert under a Kafka SSL failure, AWS
/// clock skew and web-identity denials, Pulsar's OAuth token errors, and Google's clock-skew token error. A phrase an
/// SDK upgrade changes leaves the failure unclassified, never in the wrong category.
///
/// They never key on Pinot's own wrappers, an outer plugin message, an HTTP status for AWS, a bare Kafka
/// `SerializationException` or `InvalidConfigurationException`, or which exceptions a client considers retriable.
///
/// A rule returns a verdict to decide its node, [IngestionSourceErrorVerdict#none] when the failure is not the
/// source's verdict on our request, or null to leave the node undecided.
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
  // Instance metadata endpoints serve the process's own ambient identity (AWS and Azure IMDS, the GCE metadata
  // server).
  private static final List<String> METADATA_ENDPOINTS = List.of("169.254.169.254", "metadata.google.internal");

  // ---------------------------------------------------------------------------------------------------------------
  // Evidence recorded by a client that is about to discard it

  /// The Kafka error a client records when it synthesizes "topic does not exist" from an empty metadata answer.
  static final String KAFKA_UNKNOWN_TOPIC_OR_PARTITION = "UNKNOWN_TOPIC_OR_PARTITION";

  @Nullable
  static IngestionSourceErrorVerdict matchEvidence(IngestionSourceErrorEvidence evidence) {
    String code = evidence.getCode();
    if (IngestionSourceErrorEvidence.UNREACHABLE.equals(code)) {
      return sourceRead(code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.LOCAL_PRECHECK.equals(code)) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.LOCAL_SETUP.equals(code)) {
      return none(code);
    }
    if (KAFKA_UNKNOWN_TOPIC_OR_PARTITION.equals(code)) {
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
    IngestionSourceErrorVerdict kafka = kafka(node, below, evidence);
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
      return grpc(statusCode != null ? invoke(statusCode, "getCode") : null, node, evidence);
    }
    if (hasClass(clazz, GRPC_STATUS_RUNTIME_EXCEPTION) || hasClass(clazz, GRPC_STATUS_EXCEPTION)) {
      Object status = invoke(node, "getStatus");
      return grpc(status != null ? invoke(status, "getCode") : null, node, evidence);
    }
    if (hasClass(clazz, GOOGLE_AUTH_EXCEPTION)) {
      return googleAuth(node, below, evidence);
    }
    return null;
  }

  /// Generic transport rules, applied only when no source-specific rule decided the chain.
  @Nullable
  static IngestionSourceErrorVerdict matchGeneric(Throwable node) {
    if (isUnreachable(node)) {
      return namesMetadataEndpoint(messageOf(node))
          ? none(node.getClass().getSimpleName())
          : sourceRead(node.getClass().getSimpleName(), null);
    }
    return null;
  }

  /// Whether `throwable` means the endpoint never answered: DNS, connect, TLS or a socket timeout.
  private static boolean isUnreachable(Throwable throwable) {
    return throwable instanceof UnknownHostException || throwable instanceof SocketException
        || throwable instanceof SocketTimeoutException || throwable instanceof HttpTimeoutException
        || throwable instanceof SSLException;
  }

  private static boolean namesMetadataEndpoint(String message) {
    for (String endpoint : METADATA_ENDPOINTS) {
      if (message.contains(endpoint)) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // AWS SDK v2: Kinesis, STS, KMS, Glue

  private static final String AWS_SERVICE_EXCEPTION = "software.amazon.awssdk.awscore.exception.AwsServiceException";
  private static final String AWS_CLIENT_EXCEPTION = "software.amazon.awssdk.core.exception.SdkClientException";
  private static final String AWS_STS_EXCEPTION = "software.amazon.awssdk.services.sts.model.StsException";
  private static final String AWS_ABORTED = "software.amazon.awssdk.core.exception.AbortedException";
  private static final Set<String> AWS_CALL_TIMEOUTS = Set.of(
      "software.amazon.awssdk.core.exception.ApiCallTimeoutException",
      "software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException");
  // Codes that reject a credential. Which credential decides the verdict, see `awsCredentialCode`.
  private static final Set<String> AWS_CREDENTIAL_CODES = Set.of("InvalidAccessKeyId", "SignatureDoesNotMatch",
      "InvalidSignatureException", "ExpiredToken", "ExpiredTokenException", "InvalidToken", "TokenRefreshRequired",
      "InvalidClientTokenId", "IncompleteSignature", "MissingAuthenticationToken", "UnrecognizedClientException");
  private static final Set<String> AWS_AUTHORIZATION_CODES = Set.of("RegionDisabledException",
      "GlueEncryptionException", "AccountProblem", "NotSignedUp");
  private static final Set<String> AWS_KMS_AUTHORIZATION_CODES = Set.of("KMSAccessDeniedException",
      "KMSOptInRequired", "KMSKeyNotAccessibleFault");
  private static final Set<String> AWS_SOURCE_READ_CODES = Set.of("NoSuchKey", "NoSuchBucket", "InvalidObjectState",
      "ObjectNotInActiveTierError", "PermanentRedirect", "AuthorizationHeaderMalformed",
      "IllegalLocationConstraintException", "SlowDown", "ServiceUnavailable", "ServiceUnavailableException",
      "InternalError", "InternalFailure", "Throttling", "ThrottlingException", "EntityNotFoundException",
      "InternalServiceException", "AllAccessDisabled", "InvalidPayer", "ResourceNotFoundException",
      "ResourceInUseException", "ProvisionedThroughputExceededException", "LimitExceededException");
  private static final Set<String> AWS_KMS_SOURCE_READ_CODES = Set.of("KMS.DisabledException",
      "KMS.KMSInvalidStateException", "KMS.NotFoundException", "KMSDisabledException", "KMSInvalidStateException",
      "KMSNotFoundException", "KMSThrottlingException");
  // Our clock, a request we built badly, our own web identity, or a Kinesis iterator our consumer let expire.
  private static final Set<String> AWS_NONE_CODES = Set.of("RequestTimeTooSkewed", "RequestExpired",
      "RequestInTheFuture", "ValidationError", "ValidationException", "MalformedPolicyDocument",
      "PackedPolicyTooLarge", "InvalidIdentityToken", "IDPRejectedClaim", "IDPCommunicationError",
      "InvalidInputException", "InvalidArgumentException", "ExpiredIteratorException", "ExpiredNextTokenException");
  // AWS reports our clock skew under a signature code; only the message tells it from a wrong secret.
  private static final List<String> AWS_CLOCK_SKEW = List.of("Signature expired", "Signature not yet current");

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
      if (message.contains("AssumeRoleWithWebIdentity")) {
        // Our own web identity, not the table's role.
        return none(code);
      }
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
      for (String skew : AWS_CLOCK_SKEW) {
        if (message.contains(skew)) {
          return none(code);
        }
      }
      return awsCredentialCode(sts, code, target, evidence);
    }
    if (AWS_KMS_SOURCE_READ_CODES.contains(code)) {
      return sourceRead(code, TARGET_KMS);
    }
    if (AWS_SOURCE_READ_CODES.contains(code) || status == 429 || status >= 500) {
      return sourceRead(code, target);
    }
    if (AWS_NONE_CODES.contains(code)) {
      return none(code);
    }
    return null;
  }

  /// A credential code is the table's bad credential only when the table's static keys were what got rejected: by
  /// STS on the AssumeRole call when a role is set, or by the service when none is. Our ambient identity, and the
  /// session credentials the SDK refreshes for an assumed role, are ours: the table owner's role and policy decisions
  /// arrive as `AccessDenied` instead. With no recorded credential mode the rejected credential is unknown, so it
  /// fails closed.
  private static IngestionSourceErrorVerdict awsCredentialCode(boolean sts, String code, @Nullable String target,
      @Nullable IngestionSourceErrorEvidence evidence) {
    if (evidence == null || evidence.getCredentialMode() != IngestionSourceCredentialMode.STATIC) {
      return none(code);
    }
    boolean baseKeysRejected = sts || !evidence.isRoleArnConfigured();
    return baseKeysRejected ? auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, target) : none(code);
  }

  /// No service exception sits beneath this node, or it would have decided first.
  @Nullable
  private static IngestionSourceErrorVerdict awsClient(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String message = messageOf(node);
    if (message.startsWith("Unable to load credentials") || message.contains("Failed to load credentials from IMDS")
        || hasClass(clazz, AWS_ABORTED)) {
      // The process's own credential chain, or our cancellation.
      return none(clazz.getSimpleName());
    }
    for (String timeout : AWS_CALL_TIMEOUTS) {
      if (hasClass(clazz, timeout)) {
        return sourceRead(clazz.getSimpleName(), null);
      }
    }
    for (Throwable cause : below) {
      if (isUnreachable(cause)) {
        return namesMetadataEndpoint(message) || namesMetadataEndpoint(messageOf(cause))
            ? none(cause.getClass().getSimpleName())
            : sourceRead(cause.getClass().getSimpleName(), null);
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
  private static IngestionSourceErrorVerdict kafka(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
    Class<?> clazz = node.getClass();
    String code = clazz.getSimpleName();
    if (hasClass(clazz, KAFKA_AUTHORIZATION)) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_BROKER);
    }
    if (hasClass(clazz, KAFKA_AUTHENTICATION)) {
      return kafkaAuthentication(node, below, evidence);
    }
    if (hasClass(clazz, KAFKA_THROTTLING_QUOTA)) {
      return sourceRead(code, TARGET_SCHEMA_REGISTRY);
    }
    for (String className : KAFKA_BROKER_SOURCE_READ) {
      if (hasClass(clazz, className)) {
        // A broker's timeout carries no cause. The schema-registry serde reuses these classes over the network cause
        // of its own request, so with a cause the endpoint is unknown here.
        return sourceRead(code, below.isEmpty() ? TARGET_BROKER : null);
      }
    }
    if (hasClass(clazz, KAFKA_CONFIG_EXCEPTION)) {
      // Only an unresolvable bootstrap list is the source's; any other config error is ours.
      return messageOf(node).startsWith("No resolvable bootstrap urls")
          ? sourceRead(code, TARGET_BROKER)
          : none(code);
    }
    return null;
  }

  /// Kafka's authentication classes say less than they seem to. MSK IAM reports a policy denial as a SASL
  /// authentication failure, a client-side failure to build the SASL token (our credential chain, our Kerberos setup)
  /// arrives under the same class with a `SaslException` cause, a broker's connection throttle reuses it, and an SSL
  /// failure covers both directions of a rejected certificate.
  private static IngestionSourceErrorVerdict kafkaAuthentication(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
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
        // A broker rejection carries no cause; this one failed on our side before the broker judged anything.
        return none(code);
      }
    }
    // A client that records its credential mode signs with a cloud identity (MSK IAM), whose policy denials arrive
    // as authentication failures: the class can't tell which.
    IngestionSourceErrorSubReason subReason = evidence != null && evidence.getCredentialMode() != null
        ? IngestionSourceErrorSubReason.UNSPECIFIED : IngestionSourceErrorSubReason.AUTHENTICATION;
    return auth(subReason, code, TARGET_BROKER);
  }

  private static IngestionSourceErrorVerdict kafkaTls(String code, List<Throwable> below) {
    for (Throwable cause : below) {
      for (String rejected : CERTIFICATE_REJECTED) {
        if (hasClass(cause.getClass(), rejected)) {
          // We could not verify the broker: the endpoint, not our credential.
          return sourceRead(code, TARGET_BROKER);
        }
      }
    }
    for (Throwable cause : below) {
      Matcher alert = TLS_ALERT.matcher(messageOf(cause));
      if (cause instanceof SSLException && alert.find()) {
        String name = alert.group(1);
        if (TLS_CERTIFICATE_REJECTED_ALERTS.contains(name)) {
          return auth(IngestionSourceErrorSubReason.AUTHENTICATION, name, TARGET_BROKER);
        }
        if (TLS_ACCESS_DENIED_ALERT.equals(name)) {
          return auth(IngestionSourceErrorSubReason.AUTHORIZATION, name, TARGET_BROKER);
        }
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
    String code = String.valueOf(status);
    if (status == 401) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_SCHEMA_REGISTRY);
    }
    if (status == 403) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_SCHEMA_REGISTRY);
    }
    if (status == 404 || status == 408 || status == 429 || status >= 500) {
      return sourceRead(code, TARGET_SCHEMA_REGISTRY);
    }
    // Another 4xx: a request Pinot built badly.
    return none(code);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Logins: Kerberos (Krb5LoginModule) and the Kafka OAUTHBEARER login, which both stringify their evidence

  private static final String LOGIN_EXCEPTION = "javax.security.auth.login.LoginException";
  // kafka-clients' token retriever: "The response code 401 and error response {...} was encountered ...".
  private static final Pattern OAUTH_RESPONSE_CODE = Pattern.compile("The response code (\\d{3})");
  private static final Pattern OAUTH_ERROR = Pattern.compile("\"error\"\\s*:\\s*\"([a-z_]{1,64})\"");
  private static final String OAUTH_TOKEN_UNAVAILABLE = "An internal error occurred while retrieving token";
  private static final Set<String> OAUTH_AUTHENTICATION = Set.of("invalid_client", "invalid_grant",
      "unsupported_grant_type");
  private static final Set<String> OAUTH_AUTHORIZATION = Set.of("unauthorized_client", "invalid_scope");
  private static final String OAUTH_INVALID_REQUEST = "invalid_request";
  private static final List<String> KERBEROS_UNREACHABLE = List.of("Cannot contact any KDC", "Receive timed out");
  // Our clock, keytab or krb5 setup.
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
    for (String unreachable : KERBEROS_UNREACHABLE) {
      if (message.contains(unreachable)) {
        return sourceRead("kdc_unreachable", TARGET_IDP);
      }
    }
    for (String ours : KERBEROS_LOCAL_FAILURES) {
      if (message.contains(ours)) {
        return none(node.getClass().getSimpleName());
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
      if (OAUTH_INVALID_REQUEST.equals(normalized)) {
        // A malformed request: ours.
        return none(normalized);
      }
    }
    String code = String.valueOf(status);
    if (status == 401) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_IDP);
    }
    if (status == 403) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_IDP);
    }
    if (status == 404 || status == 408 || status == 429 || status >= 500) {
      return sourceRead(code, TARGET_IDP);
    }
    return null;
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
  private static final Set<String> PULSAR_NONE = Set.of("InvalidServiceURL", "InvalidServiceURLException",
      "InvalidConfigurationException", "InvalidTopicNameException", "UnsupportedAuthenticationException",
      "AlreadyClosedException", "NotSupportedException", "CryptoException", "ChecksumException",
      "GettingAuthenticationDataException");
  // The OAuth2 plugin's token errors: "Unable to obtain an access token: <description> (<error>)".
  private static final String PULSAR_OAUTH_TOKEN = "Unable to obtain an access token";
  private static final Pattern PULSAR_OAUTH_ERROR = Pattern.compile("\\(([a-z_]{1,64})\\)\\s*$");

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
      if (simple.equals("TooManyRequestsException") && message.contains("Requests number out of config")) {
        // The client's own lookup semaphore, not the broker's throttle.
        return none(simple);
      }
      if (PULSAR_SOURCE_READ.contains(simple)) {
        return sourceRead(simple, TARGET_BROKER);
      }
      if (PULSAR_NONE.contains(simple)) {
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
      return none(code);
    }
    if (message.contains("Unable to retrieve OAuth 2.0 server metadata")) {
      return sourceRead(code, TARGET_IDP);
    }
    if (message.contains(PULSAR_OAUTH_TOKEN)) {
      Matcher error = PULSAR_OAUTH_ERROR.matcher(message);
      IngestionSourceErrorVerdict verdict = error.find() ? oauth(error.group(1), 0) : null;
      // Without an OAuth error the token endpoint returned another status or never answered.
      return verdict != null ? verdict : sourceRead(code, TARGET_IDP);
    }
    // The broker rejected the token: it gives one generic message for every reason.
    return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_BROKER);
  }

  /// The admin client maps both 401 and 403 to `NotAuthorizedException`: only the status tells them apart.
  @Nullable
  private static IngestionSourceErrorVerdict pulsarAdmin(Throwable node) {
    String simple = node.getClass().getName().substring(node.getClass().getName().lastIndexOf('$') + 1);
    if (simple.equals("ConnectException") || simple.equals("TimeoutException")) {
      return sourceRead(simple, TARGET_ADMIN);
    }
    if (simple.equals("GettingAuthenticationDataException")) {
      return none(simple);
    }
    Object statusValue = invoke(node, "getStatusCode");
    int status = statusValue instanceof Integer ? (Integer) statusValue : 0;
    String code = String.valueOf(status);
    if (status == 401) {
      return auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, TARGET_ADMIN);
    }
    if (status == 403) {
      return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, TARGET_ADMIN);
    }
    if (status == 404 || status == 429 || status >= 500) {
      return sourceRead(code, TARGET_ADMIN);
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // gRPC and gax (Google Cloud clients), and google-auth

  private static final String GAX_API_EXCEPTION = "com.google.api.gax.rpc.ApiException";
  private static final String GRPC_STATUS_RUNTIME_EXCEPTION = "io.grpc.StatusRuntimeException";
  private static final String GRPC_STATUS_EXCEPTION = "io.grpc.StatusException";
  private static final String GOOGLE_AUTH_EXCEPTION = "com.google.auth.oauth2.GoogleAuthException";
  private static final Set<String> GRPC_SOURCE_READ = Set.of("NOT_FOUND", "FAILED_PRECONDITION",
      "RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL");
  private static final Set<String> GRPC_NONE = Set.of("CANCELLED", "INVALID_ARGUMENT", "ALREADY_EXISTS");
  // Google Cloud's deliberate conflation of a missing project with a denied one.
  private static final String GOOGLE_MISSING_OR_DENIED = "not found or user does not have access";
  // "Token must be a short-lived token ... Check your iat and exp values": our clock.
  private static final String GOOGLE_CLOCK_SKEW = "short-lived token";

  @Nullable
  private static IngestionSourceErrorVerdict grpc(@Nullable Object codeValue, Throwable node,
      @Nullable IngestionSourceErrorEvidence evidence) {
    if (codeValue == null) {
      return null;
    }
    String code = codeValue.toString();
    switch (code) {
      case "UNAUTHENTICATED":
        // A rejected credential is the table owner's only when it was the table's: under the process's own
        // identity, grpc-auth reports our failed token refresh this way.
        return evidence != null && evidence.getCredentialMode() == IngestionSourceCredentialMode.STATIC
            ? auth(IngestionSourceErrorSubReason.AUTHENTICATION, code, null) : none(code);
      case "PERMISSION_DENIED":
        return auth(IngestionSourceErrorSubReason.AUTHORIZATION, code, null);
      case "NOT_FOUND":
        if (messageOf(node).contains(GOOGLE_MISSING_OR_DENIED)) {
          return auth(IngestionSourceErrorSubReason.MISSING_OR_DENIED, code, null);
        }
        return sourceRead(code, null);
      default:
        if (GRPC_SOURCE_READ.contains(code)) {
          return sourceRead(code, null);
        }
        return GRPC_NONE.contains(code) ? none(code) : null;
    }
  }

  /// A token refresh that failed. Under the process's own identity the token is ours to get; our clock skew is ours
  /// whoever's key it is. Otherwise the status the transport built above it decides.
  @Nullable
  private static IngestionSourceErrorVerdict googleAuth(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
    String code = node.getClass().getSimpleName();
    if (evidence != null && evidence.getCredentialMode() == IngestionSourceCredentialMode.AMBIENT) {
      return none(code);
    }
    if (messageOf(node).contains(GOOGLE_CLOCK_SKEW)) {
      return none(code);
    }
    for (Throwable cause : below) {
      Object content = invoke(cause, "getContent");
      String body = content != null ? content.toString() : messageOf(cause);
      if (body.contains(GOOGLE_CLOCK_SKEW)) {
        return none(code);
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
    return IngestionSourceErrorRule.hasClass(clazz, className);
  }

  private static String messageOf(Throwable throwable) {
    return IngestionSourceErrorRule.messageOf(throwable);
  }

  @Nullable
  private static Object invoke(Object target, String methodName) {
    return IngestionSourceErrorRule.invoke(target, methodName);
  }
}
