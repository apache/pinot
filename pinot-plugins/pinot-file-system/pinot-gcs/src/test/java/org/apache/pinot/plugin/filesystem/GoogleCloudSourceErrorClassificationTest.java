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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.SecurityLevel;
import io.grpc.Status;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.security.KeyPairGenerator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLHandshakeException;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_IDP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Pins the classifier's gRPC, gax and google-auth rules against the Google Cloud client libraries Pinot ships. A
/// rejected token is AUTH whoever's key it was, our clock skew included; a token endpoint that failed is SOURCE_READ.
public class GoogleCloudSourceErrorClassificationTest {
  private static final String CLOCK_SKEW = "{\"error\":\"invalid_grant\",\"error_description\":\"Invalid JWT: "
      + "Token must be a short-lived token (60 minutes) and in a reasonable timeframe. Check your iat and exp values "
      + "in the JWT claim.\"}";
  private static final String KEY_REVOKED = "{\"error\":\"invalid_grant\",\"error_description\":\"Invalid JWT "
      + "Signature.\"}";

  private static Throwable gax(Status.Code code, String message) {
    return ApiExceptionFactory.createException(message, null, GrpcStatusCode.of(code), false);
  }

  private static final MethodDescriptor.Marshaller<Object> UNUSED = new MethodDescriptor.Marshaller<>() {
    @Override
    public InputStream stream(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object parse(InputStream stream) {
      throw new UnsupportedOperationException();
    }
  };
  private static final CallCredentials.RequestInfo PULL = new CallCredentials.RequestInfo() {
    @Override
    public MethodDescriptor<?, ?> getMethodDescriptor() {
      return MethodDescriptor.newBuilder(UNUSED, UNUSED).setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("google.pubsub.v1.Subscriber/Pull").build();
    }

    @Override
    public SecurityLevel getSecurityLevel() {
      return SecurityLevel.PRIVACY_AND_INTEGRITY;
    }

    @Override
    public String getAuthority() {
      return "pubsub.googleapis.com";
    }

    @Override
    public Attributes getTransportAttrs() {
      return Attributes.EMPTY;
    }
  };

  private static Throwable tokenRefreshFailure(String tokenEndpointBody)
      throws Exception {
    return tokenRefreshFailure(tokenEndpoint(400, tokenEndpointBody));
  }

  private static HttpTransport tokenEndpoint(int status, String body) {
    return new MockHttpTransport.Builder().setLowLevelHttpResponse(new MockLowLevelHttpResponse().setStatusCode(status)
        .setContentType("application/json").setContent(body)).build();
  }

  /// What a gRPC call reports when the token refresh behind it fails, as grpc-auth builds it from the failure.
  private static Throwable tokenRefreshFailure(HttpTransport tokenEndpoint)
      throws Exception {
    ServiceAccountCredentials credentials = ServiceAccountCredentials.newBuilder()
        .setClientEmail("fixture@fixture-project.iam.gserviceaccount.com").setPrivateKeyId("fixture")
        .setPrivateKey(KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate())
        .setScopes(List.of("https://www.googleapis.com/auth/pubsub"))
        .setHttpTransportFactory(() -> tokenEndpoint)
        // google-auth's backoff would only slow the test: retrying doesn't change which failure is reported
        .setDefaultRetriesEnabled(false)
        .build();
    return callFailure(credentials);
  }

  /// What a gRPC call reports when an impersonated credential's IAM token request fails: google-auth wraps the HTTP
  /// response in a plain IOException, with no GoogleAuthException between.
  private static Throwable impersonationFailure(int status)
      throws Exception {
    GoogleCredentials source =
        GoogleCredentials.create(new AccessToken("fixture-token", new Date(System.currentTimeMillis() + 3_600_000L)));
    ImpersonatedCredentials credentials = ImpersonatedCredentials.newBuilder().setSourceCredentials(source)
        .setTargetPrincipal("target@fixture-project.iam.gserviceaccount.com")
        .setScopes(List.of("https://www.googleapis.com/auth/pubsub"))
        .setHttpTransportFactory(() -> tokenEndpoint(status, "{\"error\":{\"code\":" + status + "}}"))
        .build();
    return callFailure(credentials);
  }

  private static Throwable callFailure(Credentials credentials) {
    AtomicReference<Status> failed = new AtomicReference<>();
    MoreCallCredentials.from(credentials).applyRequestMetadata(PULL, Runnable::run,
        new CallCredentials.MetadataApplier() {
          @Override
          public void apply(Metadata headers) {
          }

          @Override
          public void fail(Status status) {
            failed.set(status);
          }
        });
    assertNotNull(failed.get(), "Expected the token refresh to fail");
    return failed.get().asRuntimeException();
  }

  @Test
  public void rejectedTokenIsAuth()
      throws Exception {
    assertAuth(tokenRefreshFailure(KEY_REVOKED), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // Our clock skew is refused the same way: no body decides against AUTH.
    assertAuth(tokenRefreshFailure(CLOCK_SKEW), IngestionSourceErrorSubReason.AUTHENTICATION, null);
  }

  @Test
  public void tokenEndpointThatNeverAnsweredIsSourceRead()
      throws Exception {
    HttpTransport unreachable = new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest(url) {
          @Override
          public LowLevelHttpResponse execute()
              throws IOException {
            throw new ConnectException("Connection refused");
          }
        };
      }
    };
    assertSourceRead(tokenRefreshFailure(unreachable), null);
    // An impersonated credential whose IAM endpoint failed, or refused.
    assertEquals(assertSourceRead(impersonationFailure(503), TARGET_IDP).getCode(), "503");
    assertAuth(impersonationFailure(403), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // grpc-auth reports a metadata server it couldn't find as UNAUTHENTICATED, over the network failure.
    assertSourceRead(Status.UNAUTHENTICATED.withCause(new IOException(
        "ComputeEngineCredentials cannot find the metadata server.", new UnknownHostException(
            "metadata.google.internal"))).asRuntimeException(), TARGET_IDP);
  }

  @Test
  public void tokenEndpointThatFailedIsSourceRead()
      throws Exception {
    // google-auth won't retry a 502 or 504, so grpc-auth reports it as UNAUTHENTICATED; a 500 or 503 as UNAVAILABLE.
    for (int status : new int[]{502, 504, 500, 503}) {
      assertSourceRead(tokenRefreshFailure(tokenEndpoint(status, "Bad Gateway")), TARGET_IDP);
    }
  }

  @Test
  public void permissionDenied() {
    assertAuth(new RuntimeException("Failed to create a subscription", gax(Status.Code.PERMISSION_DENIED,
        "User not authorized to perform this action.")), IngestionSourceErrorSubReason.AUTHORIZATION, null);
    assertAuth(Status.PERMISSION_DENIED.withDescription("Request is prohibited by organization's policy")
        .asRuntimeException(), IngestionSourceErrorSubReason.AUTHORIZATION, null);
  }

  @Test
  public void peerRejectingOurCertificateBeneathUnavailable() {
    // gRPC surfaces a TLS handshake the server refused as UNAVAILABLE with the SSL failure as its cause
    assertAuth(Status.UNAVAILABLE.withCause(new SSLHandshakeException("Received fatal alert: bad_certificate"))
        .asRuntimeException(), IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertSourceRead(Status.UNAVAILABLE.withCause(new SSLHandshakeException("Received fatal alert: handshake_failure"))
        .asRuntimeException(), null);
  }

  @Test
  public void missingOrDeniedProject() {
    assertAuth(gax(Status.Code.NOT_FOUND, "Requested project not found or user does not have access to it."),
        IngestionSourceErrorSubReason.MISSING_OR_DENIED, null);
  }

  @Test
  public void sourceCannotServeUs() {
    assertSourceRead(gax(Status.Code.NOT_FOUND, "Resource not found (resource=events)."), null);
    assertSourceRead(gax(Status.Code.FAILED_PRECONDITION, "The resulting snapshot would expire in less than 1 hour"),
        null);
    assertSourceRead(gax(Status.Code.RESOURCE_EXHAUSTED, "Quota exceeded"), null);
    assertSourceRead(Status.UNAVAILABLE.withDescription("io exception").asRuntimeException(), null);
    assertSourceRead(gax(Status.Code.DEADLINE_EXCEEDED, "Deadline exceeded"), null);
    assertSourceRead(gax(Status.Code.INVALID_ARGUMENT, "Invalid resource name"), null);
  }

  @Test
  public void ourCancellationIsNotClassified() {
    assertNotClassified(gax(Status.Code.CANCELLED, "Call cancelled"));
  }
}
