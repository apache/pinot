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


final class IngestionSourceErrorRules {
  private IngestionSourceErrorRules() {
  }

  private static final String TARGET_ADMIN = IngestionSourceErrorVerdict.TARGET_ADMIN;
  private static final String TARGET_BROKER = IngestionSourceErrorVerdict.TARGET_BROKER;
  private static final String TARGET_IDP = IngestionSourceErrorVerdict.TARGET_IDP;
  private static final String TARGET_KMS = IngestionSourceErrorVerdict.TARGET_KMS;
  private static final String TARGET_SCHEMA_REGISTRY = IngestionSourceErrorVerdict.TARGET_SCHEMA_REGISTRY;
  private static final String TARGET_STS = IngestionSourceErrorVerdict.TARGET_STS;

  private static final Set<String> INTERRUPTS = Set.of("java.lang.InterruptedException",
      "java.io.InterruptedIOException", "java.nio.channels.ClosedByInterruptException",
      "java.util.concurrent.CancellationException");

  @Nullable
  static IngestionSourceErrorVerdict matchEvidence(IngestionSourceErrorEvidence evidence) {
    String code = evidence.getCode();
    if (IngestionSourceErrorEvidence.UNREACHABLE.equals(code)) {
      return sourceRead(code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.LOCAL_PRECHECK.equals(code)) {
      return auth(code, evidence.getTarget());
    }
    if (IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION.equals(code)) {
      return sourceRead(code, evidence.getTarget() != null ? evidence.getTarget() : TARGET_BROKER);
    }
    return null;
  }

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
      return pulsar(node, below);
    }
    if (hasClass(clazz, PULSAR_ADMIN_EXCEPTION)) {
      return pulsarAdmin(node, below);
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

  @Nullable
  static IngestionSourceErrorVerdict matchGeneric(Throwable node) {
    IngestionSourceErrorVerdict refused = tlsAlert(node, null);
    if (refused != null) {
      return refused;
    }
    return isNetworkFailure(node) ? sourceRead(node.getClass().getSimpleName(), null) : null;
  }

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
    return TLS_REFUSAL_ALERTS.contains(name) ? auth(name, target) : null;
  }

  private static IngestionSourceErrorVerdict credentialFailure(String code, @Nullable String target,
      List<Throwable> below) {
    for (Throwable cause : below) {
      IngestionSourceErrorVerdict refused = tlsAlert(cause, TARGET_IDP);
      if (refused != null) {
        return refused;
      }
      if (isNetworkFailure(cause) || hasAnyClass(cause.getClass(), APACHE_NO_HTTP_RESPONSE)) {
        return sourceRead(cause.getClass().getSimpleName(), TARGET_IDP);
      }
      Object status = hasClass(cause.getClass(), GOOGLE_HTTP_RESPONSE_EXCEPTION) ? invoke(cause, "getStatusCode")
          : hasClass(cause.getClass(), AWS_CORE_SERVICE_EXCEPTION) ? invoke(cause, "statusCode") : null;
      if (status instanceof Integer && isUnavailable((Integer) status)) {
        return sourceRead(String.valueOf(status), TARGET_IDP);
      }
    }
    return auth(code, target);
  }

  private static boolean isNetworkFailure(Throwable throwable) {
    return throwable instanceof UnknownHostException || throwable instanceof SocketException
        || throwable instanceof SocketTimeoutException || throwable instanceof HttpTimeoutException
        || throwable instanceof SSLException;
  }

  private static final String AWS_SERVICE_EXCEPTION = "software.amazon.awssdk.awscore.exception.AwsServiceException";
  private static final String AWS_CLIENT_EXCEPTION = "software.amazon.awssdk.core.exception.SdkClientException";
  private static final String AWS_STS_EXCEPTION = "software.amazon.awssdk.services.sts.model.StsException";
  private static final String AWS_ABORTED = "software.amazon.awssdk.core.exception.AbortedException";
  private static final String AWS_CORE_SERVICE_EXCEPTION = "software.amazon.awssdk.core.exception.SdkServiceException";
  private static final Set<String> APACHE_NO_HTTP_RESPONSE = Set.of("org.apache.http.NoHttpResponseException",
      "org.apache.hc.core5.http.NoHttpResponseException");
  private static final Set<String> AWS_CALL_TIMEOUTS = Set.of(
      "software.amazon.awssdk.core.exception.ApiCallTimeoutException",
      "software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException");
  private static final Set<String> AWS_AUTH_CODES = Set.of("InvalidAccessKeyId", "SignatureDoesNotMatch",
      "InvalidSignatureException", "ExpiredToken", "ExpiredTokenException", "InvalidToken", "TokenRefreshRequired",
      "InvalidClientTokenId", "IncompleteSignature", "MissingAuthenticationToken", "UnrecognizedClientException",
      "RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture", "InvalidIdentityToken", "IDPRejectedClaim",
      "RegionDisabledException", "GlueEncryptionException", "AccountProblem", "NotSignedUp", "AllAccessDisabled",
      "InvalidPayer");
  private static final Set<String> AWS_KMS_AUTH_CODES = Set.of("KMSAccessDeniedException",
      "KMSOptInRequired", "KMSKeyNotAccessibleFault");
  private static final Set<String> AWS_SOURCE_READ_CODES = Set.of("NoSuchKey", "NoSuchBucket", "InvalidObjectState",
      "ObjectNotInActiveTierError", "PermanentRedirect", "AuthorizationHeaderMalformed",
      "IllegalLocationConstraintException", "SlowDown", "ServiceUnavailable", "ServiceUnavailableException",
      "InternalError", "InternalFailure", "Throttling", "ThrottlingException", "EntityNotFoundException",
      "InternalServiceException", "ResourceNotFoundException", "ResourceInUseException",
      "ProvisionedThroughputExceededException", "LimitExceededException", "ExpiredIteratorException",
      "ExpiredNextTokenException", "IDPCommunicationError");
  private static final Set<String> AWS_KMS_SOURCE_READ_CODES = Set.of("KMS.DisabledException",
      "KMS.KMSInvalidStateException", "KMS.NotFoundException", "KMSDisabledException", "KMSInvalidStateException",
      "KMSNotFoundException", "KMSThrottlingException");
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
      if (status == 403) {
        return auth("403", target);
      }
      if (status == 404 || status == 400 || status == 429 || (status >= 300 && status < 400) || status >= 500) {
        return sourceRead(String.valueOf(status), target);
      }
      return null;
    }
    if (code.equals("AccessDenied") || code.equals("AccessDeniedException")) {
      boolean kms = message.contains("ciphertext refers to a customer master key that does not exist")
          || message.contains("kms:");
      return auth(code, kms ? TARGET_KMS : target);
    }
    if (AWS_AUTH_CODES.contains(code)) {
      return auth(code, target);
    }
    if (AWS_KMS_AUTH_CODES.contains(code)) {
      return auth(code, TARGET_KMS);
    }
    if (AWS_KMS_SOURCE_READ_CODES.contains(code)) {
      return sourceRead(code, TARGET_KMS);
    }
    if (AWS_SOURCE_READ_CODES.contains(code) || status == 429 || status >= 500) {
      return sourceRead(code, target);
    }
    if (AWS_INVALID_REQUEST_CODES.contains(code)) {
      return sts ? auth(code, target) : sourceRead(code, target);
    }
    return null;
  }

  @Nullable
  private static IngestionSourceErrorVerdict awsClient(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String message = messageOf(node);
    if (hasClass(clazz, AWS_ABORTED)) {
      return none(clazz.getSimpleName());
    }
    if (message.startsWith("Unable to load credentials") || message.startsWith("Failed to load credentials from")) {
      return credentialFailure(clazz.getSimpleName(), null, below);
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
    return null;
  }

  private static final String KAFKA_ERRORS = "org.apache.kafka.common.errors.";
  private static final String KAFKA_AUTHENTICATION = KAFKA_ERRORS + "AuthenticationException";
  private static final String KAFKA_AUTHORIZATION = KAFKA_ERRORS + "AuthorizationException";
  private static final String KAFKA_SASL_AUTHENTICATION = KAFKA_ERRORS + "SaslAuthenticationException";
  private static final String KAFKA_SSL_AUTHENTICATION = KAFKA_ERRORS + "SslAuthenticationException";
  private static final String KAFKA_CONFIG_EXCEPTION = "org.apache.kafka.common.config.ConfigException";
  private static final Set<String> KAFKA_BROKER_SOURCE_READ = Set.of(KAFKA_ERRORS + "TimeoutException",
      KAFKA_ERRORS + "DisconnectException", KAFKA_ERRORS + "UnknownTopicOrPartitionException",
      KAFKA_ERRORS + "OffsetOutOfRangeException", "org.apache.kafka.clients.consumer.OffsetOutOfRangeException");
  private static final String KAFKA_THROTTLING_QUOTA = KAFKA_ERRORS + "ThrottlingQuotaExceededException";
  private static final String SASL_EXCEPTION = "javax.security.sasl.SaslException";
  private static final Set<String> CERTIFICATE_REJECTED = Set.of("java.security.cert.CertificateException",
      "java.security.cert.CertPathValidatorException", "sun.security.validator.ValidatorException");
  private static final Pattern TLS_ALERT = Pattern.compile("Received fatal alert: ([a-z_]+)");
  private static final Set<String> TLS_REFUSAL_ALERTS = Set.of("bad_certificate", "unsupported_certificate",
      "certificate_revoked", "certificate_expired", "certificate_unknown", "unknown_ca", "certificate_required",
      "access_denied");

  @Nullable
  private static IngestionSourceErrorVerdict kafka(Throwable node, List<Throwable> below) {
    Class<?> clazz = node.getClass();
    String code = clazz.getSimpleName();
    if (hasClass(clazz, KAFKA_AUTHORIZATION)) {
      return auth(code, TARGET_BROKER);
    }
    if (hasClass(clazz, KAFKA_AUTHENTICATION)) {
      return kafkaAuthentication(node, below);
    }
    if (hasClass(clazz, KAFKA_THROTTLING_QUOTA)) {
      return sourceRead(code, TARGET_SCHEMA_REGISTRY);
    }
    if (hasAnyClass(clazz, KAFKA_BROKER_SOURCE_READ)) {
      return sourceRead(code, below.isEmpty() ? TARGET_BROKER : null);
    }
    if (hasClass(clazz, KAFKA_CONFIG_EXCEPTION)) {
      return sourceRead(code, messageOf(node).startsWith("No resolvable bootstrap urls") ? TARGET_BROKER : null);
    }
    return null;
  }

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
        return credentialFailure(code, null, below);
      }
    }
    return auth(code, TARGET_BROKER);
  }

  private static IngestionSourceErrorVerdict kafkaTls(String code, List<Throwable> below) {
    for (Throwable cause : below) {
      if (hasAnyClass(cause.getClass(), CERTIFICATE_REJECTED)) {
        return sourceRead(code, TARGET_BROKER);
      }
    }
    for (Throwable cause : below) {
      IngestionSourceErrorVerdict refused = tlsAlert(cause, TARGET_BROKER);
      if (refused != null) {
        return refused;
      }
    }
    return sourceRead(code, TARGET_BROKER);
  }

  private static final String CONFLUENT_REST_CLIENT_EXCEPTION =
      "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException";

  @Nullable
  private static IngestionSourceErrorVerdict schemaRegistry(Throwable node) {
    Object statusValue = invoke(node, "getStatus");
    if (!(statusValue instanceof Integer)) {
      return null;
    }
    int status = (Integer) statusValue;
    IngestionSourceErrorVerdict verdict = httpStatus(status, TARGET_SCHEMA_REGISTRY);
    return verdict != null ? verdict : sourceRead(String.valueOf(status), TARGET_SCHEMA_REGISTRY);
  }

  private static final String LOGIN_EXCEPTION = "javax.security.auth.login.LoginException";
  private static final Pattern OAUTH_RESPONSE_CODE = Pattern.compile("The (?:unexpected )?response code (\\d{3})");
  private static final Pattern OAUTH_ERROR =
      Pattern.compile("error response \\{\"([^\"]+)\" -|\"error\"\\s*:\\s*\"([^\"]+)\"");
  private static final int MAX_OAUTH_ERROR_CODE_LENGTH = 64;
  private static final String OAUTH_TOKEN_UNAVAILABLE = "An internal error occurred while retrieving token";
  private static final List<String> DUMPED_UNREACHABLE = List.of("java.net.ConnectException",
      "java.net.UnknownHostException", "java.net.SocketTimeoutException", "java.net.NoRouteToHostException",
      "java.net.http.HttpTimeoutException", "javax.net.ssl.SSLHandshakeException");
  private static final Set<String> OAUTH_REFUSED = Set.of("invalid_client", "invalid_grant",
      "unsupported_grant_type", "invalid_request", "unauthorized_client", "invalid_scope", "access_denied");
  private static final Set<String> OAUTH_UNAVAILABLE = Set.of("server_error", "temporarily_unavailable");
  private static final List<String> KERBEROS_UNREACHABLE = List.of("Cannot contact any KDC", "Receive timed out");
  private static final List<String> KERBEROS_LOCAL_FAILURES = List.of("Clock skew too great",
      "Unable to obtain password", "Cannot locate default realm", "Cannot locate KDC");
  private static final Pattern KERBEROS_REJECTED = Pattern.compile("\\((6|18|24)\\)");

  @Nullable
  private static IngestionSourceErrorVerdict login(Throwable node) {
    String message = messageOf(node);
    Matcher responseCode = OAUTH_RESPONSE_CODE.matcher(message);
    if (responseCode.find()) {
      Matcher error = OAUTH_ERROR.matcher(message);
      String oauthError = error.find() ? (error.group(1) != null ? error.group(1) : error.group(2)) : null;
      return oauth(oauthError, Integer.parseInt(responseCode.group(1)));
    }
    if (message.contains(OAUTH_TOKEN_UNAVAILABLE)) {
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
        return auth(node.getClass().getSimpleName(), null);
      }
    }
    Matcher rejected = KERBEROS_REJECTED.matcher(message);
    if (rejected.find()) {
      return auth("krb5_" + rejected.group(1), TARGET_IDP);
    }
    return null;
  }

  @Nullable
  private static IngestionSourceErrorVerdict oauth(@Nullable String error, int status) {
    if (error != null) {
      String normalized = error.toLowerCase(Locale.ROOT);
      String code = normalized.length() > MAX_OAUTH_ERROR_CODE_LENGTH
          ? normalized.substring(0, MAX_OAUTH_ERROR_CODE_LENGTH) : normalized;
      if (status == 404 || isUnavailable(status) || OAUTH_UNAVAILABLE.contains(normalized)) {
        return sourceRead(code, TARGET_IDP);
      }
      if (OAUTH_REFUSED.contains(normalized) || status == 0 || status == 400) {
        return auth(code, TARGET_IDP);
      }
    }
    return httpStatus(status, TARGET_IDP);
  }

  @Nullable
  private static IngestionSourceErrorVerdict httpStatus(int status, String target) {
    String code = String.valueOf(status);
    if (status == 401 || status == 403) {
      return auth(code, target);
    }
    if (status == 404 || isUnavailable(status)) {
      return sourceRead(code, target);
    }
    return null;
  }

  private static boolean isUnavailable(int status) {
    return status == 408 || status == 429 || status >= 500;
  }

  private static final String PULSAR_CLIENT_EXCEPTION = "org.apache.pulsar.client.api.PulsarClientException";
  private static final String PULSAR_V5_CLIENT_EXCEPTION = "org.apache.pulsar.client.api.v5.PulsarClientException";
  private static final String PULSAR_ADMIN_EXCEPTION = "org.apache.pulsar.client.admin.PulsarAdminException";
  private static final Set<String> PULSAR_SOURCE_READ = Set.of("TopicDoesNotExistException", "NotFoundException",
      "SubscriptionNotFoundException", "LookupException", "ConnectException", "TimeoutException",
      "BrokerMetadataException", "BrokerPersistenceException", "TopicTerminatedException", "NotAllowedException",
      "ConsumerBusyException", "NotConnectedException");
  private static final Set<String> PULSAR_CLIENT_SOURCE_READ = Set.of("InvalidServiceURL",
      "InvalidServiceURLException", "InvalidConfigurationException", "InvalidTopicNameException",
      "NotSupportedException", "CryptoException", "ChecksumException");
  private static final Set<String> PULSAR_AUTHENTICATION_SETUP = Set.of("UnsupportedAuthenticationException",
      "GettingAuthenticationDataException");
  private static final String PULSAR_OAUTH_TOKEN = "Unable to obtain an access token";
  private static final Pattern PULSAR_OAUTH_ERROR = Pattern.compile(".* \\((.+)\\)\\s*$", Pattern.DOTALL);
  private static final Pattern PULSAR_NOT_A_REFUSAL = Pattern.compile(PULSAR_OAUTH_TOKEN
      + ": java\\.util\\.concurrent\\.ExecutionException: |\\(through reference chain: [\\w.$]*TokenResult\\[");
  private static final Pattern PULSAR_OAUTH_STATUS = Pattern.compile("res: (\\d{3})\\s*$");

  @Nullable
  private static IngestionSourceErrorVerdict pulsar(Throwable node, List<Throwable> below) {
    String message = messageOf(node);
    for (Class<?> current = node.getClass(); current != null; current = current.getSuperclass()) {
      String name = current.getName();
      if (name.equals(PULSAR_CLIENT_EXCEPTION) || name.equals(PULSAR_V5_CLIENT_EXCEPTION)) {
        return message.contains("Connection already closed") ? sourceRead("connection_closed", TARGET_BROKER) : null;
      }
      String simple = name.substring(name.lastIndexOf('$') + 1);
      if (simple.equals("AuthenticationException")) {
        return pulsarAuthentication(message);
      }
      if (simple.equals("AuthorizationException")) {
        return auth(simple, TARGET_BROKER);
      }
      if (simple.equals("TooManyRequestsException") && message.contains("Requests number out of config")) {
        return sourceRead(simple, null);
      }
      if (PULSAR_SOURCE_READ.contains(simple)) {
        return sourceRead(simple, TARGET_BROKER);
      }
      if (PULSAR_CLIENT_SOURCE_READ.contains(simple)) {
        return sourceRead(simple, null);
      }
      if (PULSAR_AUTHENTICATION_SETUP.contains(simple)) {
        return credentialFailure(simple, null, below);
      }
      if (simple.equals("AlreadyClosedException")) {
        return none(simple);
      }
    }
    return null;
  }

  private static IngestionSourceErrorVerdict pulsarAuthentication(String message) {
    String code = "AuthenticationException";
    if (message.startsWith("Unable to read private key")) {
      return auth(code, null);
    }
    if (message.contains("Unable to retrieve OAuth 2.0 server metadata")) {
      return sourceRead(code, TARGET_IDP);
    }
    if (message.contains(PULSAR_OAUTH_TOKEN)) {
      Matcher error = PULSAR_OAUTH_ERROR.matcher(message);
      if (error.find() && !PULSAR_NOT_A_REFUSAL.matcher(message).find()) {
        String oauthError = error.group(1);
        return "null".equals(oauthError) ? auth(code, TARGET_IDP) : oauth(oauthError, 0);
      }
      Matcher status = PULSAR_OAUTH_STATUS.matcher(message);
      if (status.find()) {
        IngestionSourceErrorVerdict verdict = httpStatus(Integer.parseInt(status.group(1)), TARGET_IDP);
        if (verdict != null) {
          return verdict;
        }
      }
      return sourceRead(code, TARGET_IDP);
    }
    return auth(code, TARGET_BROKER);
  }

  @Nullable
  private static IngestionSourceErrorVerdict pulsarAdmin(Throwable node, List<Throwable> below) {
    String simple = node.getClass().getSimpleName();
    if (simple.equals("ConnectException") || simple.equals("TimeoutException")) {
      return sourceRead(simple, TARGET_ADMIN);
    }
    if (PULSAR_AUTHENTICATION_SETUP.contains(simple)) {
      return credentialFailure(simple, null, below);
    }
    if (node.getClass().getName().equals(PULSAR_ADMIN_EXCEPTION) && invoke(node, "getHttpError") == null) {
      return null;
    }
    Object status = invoke(node, "getStatusCode");
    return status instanceof Integer ? httpStatus((Integer) status, TARGET_ADMIN) : null;
  }

  private static final String GAX_API_EXCEPTION = "com.google.api.gax.rpc.ApiException";
  private static final String GRPC_STATUS_RUNTIME_EXCEPTION = "io.grpc.StatusRuntimeException";
  private static final String GRPC_STATUS_EXCEPTION = "io.grpc.StatusException";
  private static final String GOOGLE_AUTH_EXCEPTION = "com.google.auth.oauth2.GoogleAuthException";
  private static final String GOOGLE_HTTP_RESPONSE_EXCEPTION = "com.google.api.client.http.HttpResponseException";
  private static final Set<String> GRPC_SOURCE_READ = Set.of("FAILED_PRECONDITION", "RESOURCE_EXHAUSTED",
      "DEADLINE_EXCEEDED", "INTERNAL", "INVALID_ARGUMENT", "ALREADY_EXISTS");
  private static final String GOOGLE_MISSING_OR_DENIED = "not found or user does not have access";

  @Nullable
  private static IngestionSourceErrorVerdict grpc(@Nullable Object codeValue, Throwable node, List<Throwable> below) {
    if (!(codeValue instanceof Enum)) {
      return null;
    }
    String code = ((Enum<?>) codeValue).name();
    switch (code) {
      case "UNAUTHENTICATED":
        return credentialFailure(code, null, below);
      case "PERMISSION_DENIED":
        return auth(code, null);
      case "NOT_FOUND":
        return messageOf(node).contains(GOOGLE_MISSING_OR_DENIED) ? auth(code, null) : sourceRead(code, null);
      case "UNAVAILABLE":
        for (Throwable cause : below) {
          IngestionSourceErrorVerdict refused = tlsAlert(cause, null);
          if (refused != null) {
            return refused;
          }
        }
        return sourceRead(code, null);
      case "CANCELLED":
        return none(code);
      default:
        return GRPC_SOURCE_READ.contains(code) ? sourceRead(code, null) : null;
    }
  }

  @Nullable
  private static IngestionSourceErrorVerdict googleAuth(List<Throwable> below) {
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

  private static IngestionSourceErrorVerdict auth(@Nullable String code, @Nullable String target) {
    return IngestionSourceErrorVerdict.auth(code, target);
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
