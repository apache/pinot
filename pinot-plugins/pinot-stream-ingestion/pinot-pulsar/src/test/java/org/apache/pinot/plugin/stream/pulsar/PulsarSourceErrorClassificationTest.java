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
package org.apache.pinot.plugin.stream.pulsar;

import java.net.ConnectException;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Pins the classifier's Pulsar rules against the client pinot-pulsar ships. Pulsar's class is its code, so the most
/// specific subclass decides, wherever it sits in the tree.
public class PulsarSourceErrorClassificationTest {
  private static final String BROKER = "broker";
  private static final String IDP = "idp";
  private static final String ADMIN = "admin";

  /// The plugin wraps the client's exception, and the consume loop wraps the plugin's.
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", new RuntimeException("Caught exception while creating Pulsar "
        + "reader", failure));
  }

  @Test
  public void brokerRejectsTheToken() {
    assertAuth(underPinot(new PulsarClientException.AuthenticationException("Failed to authenticate")),
        IngestionSourceErrorSubReason.AUTHENTICATION, BROKER);
  }

  @Test
  public void oauthRefusalBuriedUnderTheV5Bridge() {
    // Pulsar 5's OAuth bridge puts the base class on top; the specific class beneath it decides.
    Throwable refused = new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Client authentication failed (invalid_client)");
    Throwable bridged = new PulsarClientException(new RuntimeException(refused));
    assertAuth(underPinot(bridged), IngestionSourceErrorSubReason.AUTHENTICATION, IDP);
    Throwable v5 = new org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Invalid grant (invalid_grant)");
    assertAuth(v5, IngestionSourceErrorSubReason.AUTHENTICATION, IDP);
  }

  @Test
  public void identityProviderThatFailedIsSourceRead() {
    assertSourceRead(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Failed to perform HTTP request. res: 503"), IDP);
    assertSourceRead(underPinot(new PulsarClientException.AuthenticationException(
        "Unable to retrieve OAuth 2.0 server metadata")), IDP);
  }

  @Test
  public void ourKeyFileOrRequestIsNotTheSource() {
    assertNotClassified(new PulsarClientException.AuthenticationException("Unable to read private key: no such file"));
    assertNotClassified(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Missing parameter (invalid_request)"));
    assertNotClassified(new PulsarClientException.InvalidConfigurationException("bad config"));
    // The client's own lookup semaphore, not the broker's throttle.
    assertNotClassified(new PulsarClientException.TooManyRequestsException("Requests number out of config: 50000"));
  }

  @Test
  public void topicPermissionDenied() {
    assertAuth(underPinot(new PulsarClientException.AuthorizationException(
        "Client is not authorized to Get Partition Metadata")), IngestionSourceErrorSubReason.AUTHORIZATION, BROKER);
  }

  @Test
  public void brokerCannotServeUs() {
    assertSourceRead(underPinot(new PulsarClientException.TopicDoesNotExistException("Topic not found")), BROKER);
    // Subclasses of LookupException read as the lookup failure they are.
    assertSourceRead(new PulsarClientException.ServiceNotReadyException("bundle unloading"), BROKER);
    assertSourceRead(new PulsarClientException.TooManyRequestsException("lookup throttled"), BROKER);
    assertSourceRead(new PulsarClientException.TimeoutException("could not get the last message id"), BROKER);
    // A TLS failure surfaces only as a closed connection.
    assertSourceRead(new PulsarClientException("Connection already closed"), BROKER);
  }

  @Test
  public void baseClassAloneDefersToItsCause() {
    // The base class with a network cause: the generic network rule decides.
    Throwable connect = new PulsarClientException(new ConnectException("Connection refused"));
    assertEquals(classify(connect).getCategory(), IngestionSourceErrorCategory.SOURCE_READ, describe(connect));
    // HTTP lookup drops the status: nothing to decide on.
    assertNotClassified(new PulsarClientException("HTTP get request failed: Forbidden"));
  }

  @Test
  public void adminStatusTellsAuthenticationFromAuthorization() {
    // The admin client maps both 401 and 403 to NotAuthorizedException.
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Unauthorized", 401),
        IngestionSourceErrorSubReason.AUTHENTICATION, ADMIN);
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Forbidden", 403),
        IngestionSourceErrorSubReason.AUTHORIZATION, ADMIN);
    assertSourceRead(new PulsarAdminException.NotFoundException(null, "Schema not found", 404), ADMIN);
    assertSourceRead(new PulsarAdminException.ServerSideErrorException(null, "unavailable", "", 503), ADMIN);
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
