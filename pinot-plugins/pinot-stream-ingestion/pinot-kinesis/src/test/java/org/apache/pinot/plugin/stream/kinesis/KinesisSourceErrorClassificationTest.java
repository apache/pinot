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

import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.ingestion.error.IngestionSourceCredentialMode;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.AccessDeniedException;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.kinesis.model.KmsDisabledException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sts.model.StsException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Pins the classifier's AWS rules against the SDK version pinot-kinesis ships. AWS answers almost everything with
/// 400 or 403, so the error code decides, and for a code that rejects a credential, whose credential it was.
public class KinesisSourceErrorClassificationTest {
  private static final String STS = "sts";
  private static final String KMS = "kms";

  private static <T extends Throwable> T withCredentials(T failure, IngestionSourceCredentialMode mode,
      boolean roleArnConfigured) {
    return IngestionSourceErrorEvidence.attach(failure,
        new IngestionSourceErrorEvidence().setCredentialMode(mode).setRoleArnConfigured(roleArnConfigured));
  }

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
    Throwable denied = AccessDeniedException.builder().awsErrorDetails(details("Kinesis", "AccessDeniedException"))
        .statusCode(400).message("User: arn is not authorized to perform: kinesis:GetRecords on resource: stream")
        .build();
    assertAuth(underPinot(withCredentials(denied, IngestionSourceCredentialMode.STATIC, false)),
        IngestionSourceErrorSubReason.AUTHORIZATION, null);
    // A denial of the process's own identity is still a permission someone must grant.
    assertAuth(underPinot(withCredentials(denied, IngestionSourceCredentialMode.AMBIENT, false)),
        IngestionSourceErrorSubReason.AUTHORIZATION, null);

    Throwable keyDenied = KmsAccessDeniedException.builder()
        .awsErrorDetails(details("Kinesis", "KMSAccessDeniedException")).statusCode(400).message("kms denied").build();
    assertAuth(keyDenied, IngestionSourceErrorSubReason.AUTHORIZATION, KMS);
  }

  @Test
  public void staticKeysRejected() {
    Throwable unknownKey = kinesis("UnrecognizedClientException",
        "The security token included in the request is invalid.");
    assertAuth(underPinot(withCredentials(unknownKey, IngestionSourceCredentialMode.STATIC, false)),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    Throwable wrongSecret = kinesis("InvalidSignatureException",
        "The request signature we calculated does not match the signature you provided.");
    assertAuth(withCredentials(wrongSecret, IngestionSourceCredentialMode.STATIC, false),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  @Test
  public void credentialCodesOnOurOwnIdentityAreNotTheSource() {
    // The process's own identity.
    assertNotClassified(withCredentials(kinesis("UnrecognizedClientException", "invalid token"),
        IngestionSourceCredentialMode.AMBIENT, false));
    // Session credentials the SDK minted for an assumed role: our refresh failed, the role decision would be
    // AccessDenied.
    assertNotClassified(withCredentials(kinesis("ExpiredTokenException", "The security token has expired."),
        IngestionSourceCredentialMode.STATIC, true));
    // Nobody recorded whose credential it was.
    assertNotClassified(kinesis("UnrecognizedClientException", "invalid token"));
    // Our clock, under a signature code.
    assertNotClassified(withCredentials(kinesis("InvalidSignatureException",
        "Signature expired: 20260101T000000Z is now earlier than 20260101T000500Z"),
        IngestionSourceCredentialMode.STATIC, false));
  }

  @Test
  public void assumeRole() {
    // The table's role refuses us (trust policy, external id): raised raw from the Kinesis call.
    Throwable trustDenied = sts("AccessDenied", 403,
        "User: arn:aws:sts::000000000000:assumed-role/node/i-0 is not authorized to perform: sts:AssumeRole on "
            + "resource: arn:aws:iam::111111111111:role/table");
    assertAuth(underPinot(withCredentials(trustDenied, IngestionSourceCredentialMode.AMBIENT, true)),
        IngestionSourceErrorSubReason.AUTHORIZATION, STS);
    // The table's base keys are rejected by STS.
    assertAuth(withCredentials(sts("InvalidClientTokenId", 403, "The security token included in the request is "
        + "invalid."), IngestionSourceCredentialMode.STATIC, true), IngestionSourceErrorSubReason.AUTHENTICATION, STS);
    // Our own web identity, reused by the default chain after its first success.
    assertNotClassified(sts("AccessDenied", 403,
        "Not authorized to perform sts:AssumeRoleWithWebIdentity"));
    assertNotClassified(sts("InvalidIdentityToken", 400, "Couldn't retrieve verification key"));
  }

  @Test
  public void streamCannotServeUs() {
    assertSourceRead(underPinot(ResourceNotFoundException.builder()
        .awsErrorDetails(details("Kinesis", "ResourceNotFoundException")).statusCode(400)
        .message("Stream events under account 000000000000 not found.").build()), null);
    assertSourceRead(ProvisionedThroughputExceededException.builder()
        .awsErrorDetails(details("Kinesis", "ProvisionedThroughputExceededException")).statusCode(400)
        .message("Rate exceeded").build(), null);
    assertSourceRead(LimitExceededException.builder().awsErrorDetails(details("Kinesis", "LimitExceededException"))
        .statusCode(400).message("Rate exceeded").build(), null);
    assertSourceRead(KmsDisabledException.builder().awsErrorDetails(details("Kinesis", "KMSDisabledException"))
        .statusCode(400).message("disabled").build(), KMS);
    assertSourceRead(kinesis("InternalFailure", "internal"), null);
    // An unmodeled 5xx.
    assertSourceRead(KinesisException.builder().awsErrorDetails(details("Kinesis", "SomethingNew")).statusCode(503)
        .message("unavailable").build(), null);
  }

  @Test
  public void glueCodesKeyOnTheCodeNotTheStatus() {
    // Glue is a JSON-protocol service: denial, missing entity and throttling are all 400.
    assertAuth(withCredentials(AwsServiceException.builder().awsErrorDetails(details("Glue", "AccessDeniedException"))
            .statusCode(400).message("denied").build(), IngestionSourceCredentialMode.STATIC, false),
        IngestionSourceErrorSubReason.AUTHORIZATION, null);
    assertSourceRead(AwsServiceException.builder().awsErrorDetails(details("Glue", "EntityNotFoundException"))
        .statusCode(400).message("Schema version is not found.").build(), null);
    assertSourceRead(AwsServiceException.builder().awsErrorDetails(details("Glue", "ThrottlingException"))
        .statusCode(400).message("Rate exceeded").build(), null);
  }

  @Test
  public void ourRequestOrIteratorIsNotTheSource() {
    assertNotClassified(ExpiredIteratorException.builder()
        .awsErrorDetails(details("Kinesis", "ExpiredIteratorException")).statusCode(400).message("expired").build());
    assertNotClassified(kinesis("InvalidArgumentException", "did not come from this stream"));
  }

  @Test
  public void clientSideFailures() {
    // The process's own credential chain.
    assertNotClassified(underPinot(SdkClientException.create(
        "Unable to load credentials from any of the providers in the chain AwsCredentialsProviderChain")));
    assertNotClassified(SdkClientException.create("Unable to execute HTTP request",
        new UnknownHostException("169.254.169.254")));
    // The endpoint never answered.
    assertSourceRead(underPinot(SdkClientException.create("Unable to execute HTTP request: kinesis.example",
        new UnknownHostException("kinesis.us-east-1.amazonaws.example"))), null);
    assertSourceRead(ApiCallTimeoutException.create(5000), null);
  }

  @Test
  public void retryRecordsAreNotNetworkFailures() {
    // The SDK attaches one cause-less record per earlier attempt; they must not read as an unreachable endpoint.
    Throwable denied = AccessDeniedException.builder().awsErrorDetails(details("Kinesis", "AccessDeniedException"))
        .statusCode(400).message("denied").build();
    denied.addSuppressed(SdkClientException.create("Request attempt 1 failure: Unable to execute HTTP request"));
    assertAuth(denied, IngestionSourceErrorSubReason.AUTHORIZATION, null);

    Throwable credentialsGone = SdkClientException.create("Unable to load credentials from any of the providers");
    credentialsGone.addSuppressed(SdkClientException.create("Request attempt 1 failure: Connect timed out"));
    assertNotClassified(credentialsGone);
  }

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
}
