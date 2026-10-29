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

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLHandshakeException;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.AccessDeniedException;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.kinesis.model.KmsDisabledException;
import software.amazon.awssdk.services.kinesis.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kinesis.model.KmsNotFoundException;
import software.amazon.awssdk.services.kinesis.model.KmsOptInRequiredException;
import software.amazon.awssdk.services.kinesis.model.KmsThrottlingException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.RegionDisabledException;
import software.amazon.awssdk.services.sts.model.StsException;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_IDP;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_KMS;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_STS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Pins the classifier's AWS rules (the code table and the message carve-outs) against the exception hierarchy of the
/// SDK pinot-kinesis ships. AWS answers almost everything with 400 or 403, so the error code decides, never whose
/// credential it rejected. Each code below is a row of the rule table that the batch classifier shares.
public class KinesisSourceErrorClassificationTest {

  private static AwsErrorDetails details(String service, String code) {
    return AwsErrorDetails.builder().serviceName(service).errorCode(code).build();
  }

  private static KinesisException kinesis(String code, String message) {
    return (KinesisException) KinesisException.builder().awsErrorDetails(details("Kinesis", code)).statusCode(400)
        .message(message).build();
  }

  private static StsException sts(String code, int status, String message) {
    return (StsException) StsException.builder().awsErrorDetails(details("Sts", code)).statusCode(status)
        .message(message).build();
  }

  /// The consume loop sees the SDK's exception raw, under Pinot's own wrappers.
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", failure);
  }

  @Test
  public void permissionDenied() {
    assertAuth(underPinot(AccessDeniedException.builder().awsErrorDetails(details("Kinesis", "AccessDeniedException"))
            .statusCode(400).message("User: arn is not authorized to perform: kinesis:GetRecords on resource: stream")
            .build()), IngestionSourceErrorSubReason.AUTHORIZATION, null);
    // The denied action names the key that encrypts the stream.
    assertAuth(kinesis("AccessDeniedException", "User: arn is not authorized to perform: kms:Decrypt on resource: "
        + "key"), IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_KMS);
    assertAuth(kinesis("AccessDeniedException", "The ciphertext refers to a customer master key that does not exist, "
            + "does not exist in this region, or you are not allowed to access."),
        IngestionSourceErrorSubReason.MISSING_OR_DENIED, TARGET_KMS);
  }

  @Test
  public void keyThatEncryptsTheStream() {
    assertAuth(KmsAccessDeniedException.builder().awsErrorDetails(details("Kinesis", "KMSAccessDeniedException"))
        .statusCode(400).message("denied").build(), IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_KMS);
    assertAuth(KmsOptInRequiredException.builder().awsErrorDetails(details("Kinesis", "KMSOptInRequired"))
        .statusCode(400).message("opt in").build(), IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_KMS);
    assertSourceRead(KmsDisabledException.builder().awsErrorDetails(details("Kinesis", "KMSDisabledException"))
        .statusCode(400).message("disabled").build(), TARGET_KMS);
    assertSourceRead(KmsInvalidStateException.builder()
        .awsErrorDetails(details("Kinesis", "KMSInvalidStateException")).statusCode(400).message("pending deletion")
        .build(), TARGET_KMS);
    assertSourceRead(KmsNotFoundException.builder().awsErrorDetails(details("Kinesis", "KMSNotFoundException"))
        .statusCode(400).message("not found").build(), TARGET_KMS);
    assertSourceRead(KmsThrottlingException.builder().awsErrorDetails(details("Kinesis", "KMSThrottlingException"))
        .statusCode(400).message("throttled").build(), TARGET_KMS);
  }

  @Test
  public void credentialCodesAreAuthWhoeverOwnsTheCredential() {
    assertAuth(underPinot(kinesis("UnrecognizedClientException",
        "The security token included in the request is invalid.")), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(kinesis("InvalidSignatureException",
            "The request signature we calculated does not match the signature you provided."),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(kinesis("ExpiredTokenException", "The security token has expired."),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // Our clock, under a signature code or its own: no message decides against AUTH.
    assertAuth(kinesis("InvalidSignatureException",
            "Signature expired: 20260101T000000Z is now earlier than 20260101T000500Z"),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(kinesis("RequestExpired", "Request has expired."), IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  @Test
  public void assumeRole() {
    // The table's role refuses us (trust policy, external id): raised raw from the Kinesis call.
    Throwable trustDenied = sts("AccessDenied", 403,
        "User: arn:aws:sts::000000000000:assumed-role/node/i-0 is not authorized to perform: sts:AssumeRole on "
            + "resource: arn:aws:iam::111111111111:role/table");
    assertAuth(underPinot(trustDenied), IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_STS);
    // The base keys are rejected by STS.
    assertAuth(sts("InvalidClientTokenId", 403, "The security token included in the request is invalid."),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_STS);
    assertAuth(RegionDisabledException.builder().awsErrorDetails(details("Sts", "RegionDisabledException"))
        .statusCode(403).message("STS is not activated in this region").build(),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_STS);
    // The process's web identity, reused by the default chain after its first success.
    assertAuth(sts("AccessDenied", 403, "Not authorized to perform sts:AssumeRoleWithWebIdentity"),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_STS);
    assertAuth(sts("InvalidIdentityToken", 400, "No OpenIDConnect provider found in your account for https://oidc"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_STS);
    // STS refusing the AssumeRole request itself: a role ARN or session duration it won't accept.
    assertAuth(sts("ValidationError", 400, "The requested DurationSeconds exceeds the MaxSessionDuration"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_STS);
    // The web identity's provider never answered STS.
    assertSourceRead(sts("IDPCommunicationError", 400, "The IdP could not be contacted"), TARGET_STS);
  }

  @Test
  public void streamCannotServeUs() {
    assertSourceRead(underPinot(ResourceNotFoundException.builder()
        .awsErrorDetails(details("Kinesis", "ResourceNotFoundException")).statusCode(400)
        .message("Stream events under account 000000000000 not found.").build()), null);
    assertSourceRead(ResourceInUseException.builder().awsErrorDetails(details("Kinesis", "ResourceInUseException"))
        .statusCode(400).message("Stream events is being updated").build(), null);
    assertSourceRead(ProvisionedThroughputExceededException.builder()
        .awsErrorDetails(details("Kinesis", "ProvisionedThroughputExceededException")).statusCode(400)
        .message("Rate exceeded").build(), null);
    assertSourceRead(LimitExceededException.builder().awsErrorDetails(details("Kinesis", "LimitExceededException"))
        .statusCode(400).message("Rate exceeded").build(), null);
    assertSourceRead(kinesis("InternalFailure", "internal"), null);
    // An unmodeled 5xx.
    assertSourceRead(KinesisException.builder().awsErrorDetails(details("Kinesis", "SomethingNew")).statusCode(503)
        .message("unavailable").build(), null);
  }

  @Test
  public void accessTheSourceDisabledIsAuthorization() {
    // S3 answers 403 when it disabled all access to a resource, or to a Requester Pays object for this requester.
    assertAuth(AwsServiceException.builder().awsErrorDetails(details("S3", "AllAccessDisabled")).statusCode(403)
        .message("All access to this Amazon S3 resource has been disabled.").build(),
        IngestionSourceErrorSubReason.AUTHORIZATION, null);
    assertAuth(AwsServiceException.builder().awsErrorDetails(details("S3", "InvalidPayer")).statusCode(403)
        .message("All access to this object has been disabled.").build(), IngestionSourceErrorSubReason.AUTHORIZATION,
        null);
  }

  @Test
  public void glueCodesKeyOnTheCodeNotTheStatus() {
    // Glue is a JSON-protocol service: denial, missing entity and throttling are all 400.
    assertAuth(AwsServiceException.builder().awsErrorDetails(details("Glue", "AccessDeniedException"))
        .statusCode(400).message("denied").build(), IngestionSourceErrorSubReason.AUTHORIZATION, null);
    assertSourceRead(AwsServiceException.builder().awsErrorDetails(details("Glue", "EntityNotFoundException"))
        .statusCode(400).message("Schema version is not found.").build(), null);
    assertSourceRead(AwsServiceException.builder().awsErrorDetails(details("Glue", "ThrottlingException"))
        .statusCode(400).message("Rate exceeded").build(), null);
  }

  @Test
  public void invalidRequestOrExpiredIteratorIsSourceRead() {
    assertSourceRead(ExpiredIteratorException.builder()
        .awsErrorDetails(details("Kinesis", "ExpiredIteratorException")).statusCode(400).message("expired").build(),
        null);
    assertSourceRead(kinesis("InvalidArgumentException", "did not come from this stream"), null);
  }

  @Test
  public void clientSideFailures() {
    // A credential chain that found no credential.
    assertAuth(underPinot(SdkClientException.create(
            "Unable to load credentials from any of the providers in the chain AwsCredentialsProviderChain")),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // One of its providers, called directly after the chain's first success: IMDS, or the ECS / EKS Pod Identity
    // container endpoint. One whose endpoint never answered, or failed, is SOURCE_READ like any endpoint.
    assertAuth(SdkClientException.create("Failed to load credentials from IMDS.",
            new IOException("Unable to parse the credentials response")), IngestionSourceErrorSubReason.AUTHENTICATION,
        null);
    assertEquals(assertSourceRead(SdkClientException.create("Failed to load credentials from IMDS.",
        new SocketTimeoutException("Connect timed out")), TARGET_IDP).getCode(), "SocketTimeoutException");
    assertEquals(assertSourceRead(SdkClientException.create("Failed to load credentials from metadata service.",
        new ConnectException("Connection refused")), TARGET_IDP).getCode(), "ConnectException");
    assertEquals(assertSourceRead(SdkClientException.create("Failed to load credentials from IMDS.",
        SdkServiceException.builder().statusCode(500).message("Internal Server Error").build()), TARGET_IDP)
        .getCode(), "500");
    // Any endpoint that never answered, a metadata endpoint included.
    assertSourceRead(SdkClientException.create("Unable to execute HTTP request",
        new ConnectException("Connect to 169.254.169.254:80 [/169.254.169.254] failed: Connection refused")), null);
    // Our cancellation.
    assertNotClassified(AbortedException.create("Thread was interrupted"));
    // The endpoint never answered.
    assertSourceRead(underPinot(SdkClientException.create("Unable to execute HTTP request: kinesis.example",
        new UnknownHostException("kinesis.us-east-1.amazonaws.example"))), null);
    assertSourceRead(ApiCallTimeoutException.create(5000), null);
    // The endpoint answered, rejecting the client certificate we presented
    assertAuth(SdkClientException.create("Unable to execute HTTP request",
        new SSLHandshakeException("Received fatal alert: bad_certificate")),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  /// A load balancer or proxy that reads each request in full and closes the connection without answering.
  private static ServerSocket droppingEndpoint()
      throws IOException {
    ServerSocket endpoint = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread dropper = new Thread(() -> {
      while (true) {
        try (Socket connection = endpoint.accept(); InputStream request = connection.getInputStream()) {
          readRequest(request);
        } catch (IOException e) {
          if (endpoint.isClosed()) {
            return;
          }
        }
      }
    });
    dropper.setDaemon(true);
    dropper.start();
    return endpoint;
  }

  private static void readRequest(InputStream request)
      throws IOException {
    StringBuilder headers = new StringBuilder();
    while (headers.indexOf("\r\n\r\n") < 0) {
      int b = request.read();
      if (b < 0) {
        return;
      }
      headers.append((char) b);
    }
    Matcher contentLength = CONTENT_LENGTH.matcher(headers);
    long remaining = contentLength.find() ? Long.parseLong(contentLength.group(1)) : 0;
    while (remaining > 0 && request.read() >= 0) {
      remaining--;
    }
  }

  private static final Pattern CONTENT_LENGTH =
      Pattern.compile("(?i)\r\ncontent-length:\\s*(\\d+)");

  private static boolean hasCause(Throwable failure, String className) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause.getClass().getName().equals(className)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void endpointThatClosesWithoutAnsweringIsSourceRead()
      throws IOException {
    // The SDK retries, then gives up with the HTTP client's no-response failure as the cause. Kinesis is built on the
    // Apache 4 client; STS takes the SDK's default, Apache 5.
    try (ServerSocket endpoint = droppingEndpoint();
        KinesisClient kinesis = KinesisClient.builder()
            .endpointOverride(URI.create("http://127.0.0.1:" + endpoint.getLocalPort()))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .httpClientBuilder(ApacheHttpClient.builder())
            .build();
        StsClient sts = StsClient.builder()
            .endpointOverride(URI.create("http://127.0.0.1:" + endpoint.getLocalPort()))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .build()) {
      SdkClientException kinesisFailure =
          expectThrows(SdkClientException.class, () -> kinesis.listShards(request -> request.streamName("stream")));
      assertTrue(hasCause(kinesisFailure, "org.apache.http.NoHttpResponseException"));
      assertEquals(assertSourceRead(underPinot(kinesisFailure), null).getCode(), "NoHttpResponseException");

      SdkClientException stsFailure = expectThrows(SdkClientException.class, sts::getCallerIdentity);
      assertTrue(hasCause(stsFailure, "org.apache.hc.core5.http.NoHttpResponseException"));
      assertEquals(assertSourceRead(underPinot(stsFailure), null).getCode(), "NoHttpResponseException");
    }
  }

  @Test
  public void retryRecordsAreNotNetworkFailures() {
    // The SDK attaches one cause-less record per earlier attempt; they must not read as an unreachable endpoint.
    Throwable cancelled = AbortedException.create("Thread was interrupted");
    cancelled.addSuppressed(SdkClientException.create("Request attempt 1 failure: Unable to execute HTTP request"));
    cancelled.addSuppressed(SdkClientException.create("Request attempt 2 failure: Connect timed out"));
    assertNotClassified(cancelled);
  }
}
