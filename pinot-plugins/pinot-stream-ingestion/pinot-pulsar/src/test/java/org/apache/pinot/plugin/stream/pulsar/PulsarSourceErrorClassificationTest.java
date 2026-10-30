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
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertNotClassified;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.classify;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_ADMIN;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_BROKER;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_IDP;
import static org.testng.Assert.assertEquals;


/// Pins the classifier's Pulsar rules against the client pinot-pulsar ships. Pulsar's class is its code, so the most
/// specific subclass decides, wherever it sits in the tree.
public class PulsarSourceErrorClassificationTest {
  /// The plugin wraps the client's exception, and the consume loop wraps the plugin's.
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", new RuntimeException("Caught exception while creating Pulsar "
        + "reader", failure));
  }

  @Test
  public void brokerRejectsTheToken() {
    assertAuth(underPinot(new PulsarClientException.AuthenticationException("Failed to authenticate")),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_BROKER);
  }

  @Test
  public void oauthRefusalBuriedUnderTheV5Bridge() {
    // Pulsar 5's OAuth bridge puts the base class on top; the specific class beneath it decides.
    Throwable refused = new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Client authentication failed (invalid_client)");
    Throwable bridged = new PulsarClientException(new RuntimeException(refused));
    assertAuth(underPinot(bridged), IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
    Throwable v5 = new org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Invalid grant (invalid_grant)");
    assertAuth(v5, IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
  }

  @Test
  public void identityProviderThatFailedIsSourceRead() {
    assertSourceRead(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Failed to perform HTTP request. res: 503"), TARGET_IDP);
    assertSourceRead(underPinot(new PulsarClientException.AuthenticationException(
        "Unable to retrieve OAuth 2.0 server metadata")), TARGET_IDP);
    assertSourceRead(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Internal error (server_error)"), TARGET_IDP);
  }

  @Test
  public void identityProviderStatusDecidesWithoutAnOAuthError() {
    // The token client reports any status but 200, 400 and 401 by its number alone.
    assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Failed to perform HTTP request. res: 403"),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_IDP);
  }

  @Test
  public void oauthErrorsDecideByTheirMeaning() {
    // The identity provider refused the grant.
    assertAuth(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Access denied (access_denied)"), IngestionSourceErrorSubReason.AUTHORIZATION,
        TARGET_IDP);
    // The endpoint refusing the token request itself.
    assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Missing parameter (invalid_request)"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
    // An error no row names is still the endpoint refusing the token request, as is a 400 or 401 without one.
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Something new (novel_error)"), IngestionSourceErrorSubReason.UNSPECIFIED,
        TARGET_IDP).getCode(), "novel_error");
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Unauthorized (Unauthorized)"), IngestionSourceErrorSubReason.UNSPECIFIED,
        TARGET_IDP).getCode(), "unauthorized");
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: null (null)"),
        IngestionSourceErrorSubReason.UNSPECIFIED, TARGET_IDP);
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: null (vendor:denied)"), IngestionSourceErrorSubReason.UNSPECIFIED,
        TARGET_IDP).getCode(), "vendor:denied");
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: null (Bad credentials)"), IngestionSourceErrorSubReason.UNSPECIFIED,
        TARGET_IDP).getCode(), "bad_credentials");
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: null (x(y))"),
        IngestionSourceErrorSubReason.UNSPECIFIED, TARGET_IDP);
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Denied (see docs) (invalid_grant)"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP).getCode(), "invalid_grant");
    // A refusal whose description names a Java class is still a refusal
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
        + "java.lang.SecurityException: Invalid client credentials (invalid_client)"),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_IDP);
    // A transport or parsing failure in the same text is not a refusal, even when it ends in a parenthesis
    assertSourceRead(new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
        + "java.util.concurrent.ExecutionException: io.netty.channel.AbstractChannel$AnnotatedConnectException: "
        + "Connection refused: localhost/127.0.0.1:1"), TARGET_IDP);
    assertSourceRead(new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
        + "java.util.concurrent.ExecutionException: java.net.ConnectException: No appropriate protocol (protocol is "
        + "disabled or cipher suites are inappropriate)"), TARGET_IDP);
    assertSourceRead(new PulsarClientException.AuthenticationException("Unable to obtain an access token: Cannot "
        + "deserialize value of type `int` from String \"never\": not a valid `int` value (through reference chain: "
        + "org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult[\"expires_in\"])"), TARGET_IDP);
  }

  @Test
  public void clientSideFailures() {
    // Authentication failed before the broker judged anything.
    assertAuth(new PulsarClientException.AuthenticationException("Unable to read private key: no such file"),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    assertAuth(new PulsarClientException.UnsupportedAuthenticationException("bad auth plugin"),
        IngestionSourceErrorSubReason.AUTHENTICATION, null);
    // Its authentication data's endpoint never answered: SOURCE_READ, as for any endpoint.
    assertSourceRead(new PulsarClientException.GettingAuthenticationDataException(
        new ConnectException("Connection refused")), TARGET_IDP);
    // The client's config: the read failed all the same.
    assertSourceRead(new PulsarClientException.InvalidConfigurationException("bad config"), null);
    assertSourceRead(new PulsarClientException.InvalidServiceURL("Invalid service URL", null), null);
    assertSourceRead(new PulsarClientException.InvalidTopicNameException("bad topic"), null);
    // The client's own lookup semaphore: the broker never saw the request.
    assertSourceRead(new PulsarClientException.TooManyRequestsException("Requests number out of config: 50000"),
        null);
    // A client Pinot already closed.
    assertNotClassified(new PulsarClientException.AlreadyClosedException("Client already closed"));
  }

  @Test
  public void topicPermissionDenied() {
    assertAuth(underPinot(new PulsarClientException.AuthorizationException(
            "Client is not authorized to Get Partition Metadata")), IngestionSourceErrorSubReason.AUTHORIZATION,
        TARGET_BROKER);
  }

  @Test
  public void brokerCannotServeUs() {
    assertSourceRead(underPinot(new PulsarClientException.TopicDoesNotExistException("Topic not found")),
        TARGET_BROKER);
    // Subclasses of LookupException read as the lookup failure they are.
    assertSourceRead(new PulsarClientException.ServiceNotReadyException("bundle unloading"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.TooManyRequestsException("lookup throttled"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.TimeoutException("could not get the last message id"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.NotFoundException("Not found: lookup"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.ConnectException("Disconnected from server at broker:6650"),
        TARGET_BROKER);
    assertSourceRead(new PulsarClientException.ConsumerBusyException("Exclusive consumer is already connected"),
        TARGET_BROKER);
    assertSourceRead(new PulsarClientException.NotAllowedException("Subscription type not allowed"), TARGET_BROKER);
    // A TLS failure surfaces only as a closed connection.
    assertSourceRead(new PulsarClientException("Connection already closed"), TARGET_BROKER);
  }

  @Test
  public void baseClassAloneDefersToItsCause() {
    // The base class with a network cause: the generic network rule decides.
    Throwable connect = new PulsarClientException(new ConnectException("Connection refused"));
    assertEquals(classify(connect).getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
    // HTTP lookup drops the status: nothing to decide on.
    assertNotClassified(new PulsarClientException("HTTP get request failed: Forbidden"));
  }

  @Test
  public void adminStatusTellsAuthenticationFromAuthorization() {
    // The admin client maps both 401 and 403 to NotAuthorizedException.
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Unauthorized", 401),
        IngestionSourceErrorSubReason.AUTHENTICATION, TARGET_ADMIN);
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Forbidden", 403),
        IngestionSourceErrorSubReason.AUTHORIZATION, TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException.NotFoundException(null, "Schema not found", 404), TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException.ServerSideErrorException(null, "unavailable", "", 503), TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException(null, "Request Timeout", 408), TARGET_ADMIN);
    // A client-side failure the admin client wraps with a default status of 500 is not the server's answer.
    assertNotClassified(new PulsarAdminException(new IllegalStateException("Client instance has been closed.")));
  }
}
