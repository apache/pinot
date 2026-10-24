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
package org.apache.pinot.plugin.filesystem;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.grpc.Status;
import java.io.IOException;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
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


/// Pins the classifier's gRPC, gax and google-auth rules against the Google Cloud client libraries Pinot ships. A
/// rejected token is the table owner's only when the table's own key was used, and Google reports our clock skew as
/// a token refusal.
public class GoogleCloudSourceErrorClassificationTest {
  private static final String CLOCK_SKEW = "{\"error\":\"invalid_grant\",\"error_description\":\"Invalid JWT: "
      + "Token must be a short-lived token (60 minutes) and in a reasonable timeframe. Check your iat and exp values "
      + "in the JWT claim.\"}";
  private static final String KEY_REVOKED = "{\"error\":\"invalid_grant\",\"error_description\":\"Invalid JWT "
      + "Signature.\"}";

  private static <T extends Throwable> T withCredentials(T failure, IngestionSourceCredentialMode mode) {
    return IngestionSourceErrorEvidence.attach(failure, new IngestionSourceErrorEvidence().setCredentialMode(mode));
  }

  private static Throwable gax(Status.Code code, String message) {
    return ApiExceptionFactory.createException(message, null, GrpcStatusCode.of(code), false);
  }

  /// What grpc-auth reports when the token refresh behind a call fails: UNAUTHENTICATED over the refresh's error.
  private static Throwable tokenRefreshFailure(String tokenEndpointBody)
      throws Exception {
    ServiceAccountCredentials credentials = ServiceAccountCredentials.newBuilder()
        .setClientEmail("fixture@fixture-project.iam.gserviceaccount.com").setPrivateKeyId("fixture")
        .setPrivateKey(KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate())
        .setScopes(List.of("https://www.googleapis.com/auth/pubsub"))
        .setHttpTransportFactory(() -> new MockHttpTransport.Builder().setLowLevelHttpResponse(
            new MockLowLevelHttpResponse().setStatusCode(400).setContentType("application/json")
                .setContent(tokenEndpointBody)).build())
        .build();
    try {
      credentials.refresh();
      fail("Expected the token refresh to fail");
      return null;
    } catch (IOException e) {
      return Status.UNAUTHENTICATED.withDescription("Failed computing credential metadata").withCause(e)
          .asRuntimeException();
    }
  }

  @Test
  public void tableKeyRejected()
      throws Exception {
    assertAuth(withCredentials(tokenRefreshFailure(KEY_REVOKED), IngestionSourceCredentialMode.STATIC),
        IngestionSourceErrorSubReason.AUTHENTICATION);
  }

  @Test
  public void ourIdentityOrClockIsNotTheSource()
      throws Exception {
    // The process's own credentials failing to refresh.
    assertNotClassified(withCredentials(tokenRefreshFailure(KEY_REVOKED), IngestionSourceCredentialMode.AMBIENT));
    // Our clock skew, even on the table's key.
    assertNotClassified(withCredentials(tokenRefreshFailure(CLOCK_SKEW), IngestionSourceCredentialMode.STATIC));
    // Nobody recorded whose credential it was.
    assertNotClassified(tokenRefreshFailure(KEY_REVOKED));
  }

  @Test
  public void permissionDenied() {
    assertAuth(new RuntimeException("Failed to create a subscription", gax(Status.Code.PERMISSION_DENIED,
        "User not authorized to perform this action.")), IngestionSourceErrorSubReason.AUTHORIZATION);
    assertAuth(Status.PERMISSION_DENIED.withDescription("Request is prohibited by organization's policy")
        .asRuntimeException(), IngestionSourceErrorSubReason.AUTHORIZATION);
  }

  @Test
  public void missingOrDeniedProject() {
    assertAuth(gax(Status.Code.NOT_FOUND, "Requested project not found or user does not have access to it."),
        IngestionSourceErrorSubReason.MISSING_OR_DENIED);
  }

  @Test
  public void sourceCannotServeUs() {
    assertSourceRead(gax(Status.Code.NOT_FOUND, "Resource not found (resource=events)."));
    assertSourceRead(gax(Status.Code.FAILED_PRECONDITION, "The resulting snapshot would expire in less than 1 hour"));
    assertSourceRead(gax(Status.Code.RESOURCE_EXHAUSTED, "Quota exceeded"));
    assertSourceRead(Status.UNAVAILABLE.withDescription("io exception").asRuntimeException());
    assertSourceRead(gax(Status.Code.DEADLINE_EXCEEDED, "Deadline exceeded"));
  }

  @Test
  public void ourCancellationOrRequestIsNotTheSource() {
    assertNotClassified(gax(Status.Code.CANCELLED, "Call cancelled"));
    assertNotClassified(gax(Status.Code.INVALID_ARGUMENT, "Invalid resource name"));
  }

  private static void assertAuth(Throwable failure, IngestionSourceErrorSubReason subReason) {
    IngestionSourceErrorClassification classification = classify(failure);
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.AUTH, describe(failure));
    assertEquals(classification.getSubReason(), subReason, describe(failure));
  }

  private static void assertSourceRead(Throwable failure) {
    assertEquals(classify(failure).getCategory(), IngestionSourceErrorCategory.SOURCE_READ, describe(failure));
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
