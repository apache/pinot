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


public class PulsarSourceErrorClassificationTest {
  private static Throwable underPinot(Throwable failure) {
    return new RuntimeException("Pinot wrapper", new RuntimeException("Caught exception while creating Pulsar "
        + "reader", failure));
  }

  @Test
  public void brokerRejectsTheToken() {
    assertAuth(underPinot(new PulsarClientException.AuthenticationException("Failed to authenticate")),
        TARGET_BROKER);
  }

  @Test
  public void oauthRefusalBuriedUnderTheV5Bridge() {
    Throwable refused = new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Client authentication failed (invalid_client)");
    Throwable bridged = new PulsarClientException(new RuntimeException(refused));
    assertAuth(underPinot(bridged), TARGET_IDP);
    Throwable v5 = new org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Invalid grant (invalid_grant)");
    assertAuth(v5, TARGET_IDP);
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
    assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Failed to perform HTTP request. res: 403"),
        TARGET_IDP);
  }

  @Test
  public void oauthErrorsDecideByTheirMeaning() {
    assertAuth(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: Access denied (access_denied)"), TARGET_IDP);
    assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Missing parameter (invalid_request)"),
        TARGET_IDP);
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Something new (novel_error)"), TARGET_IDP).getCode(), "novel_error");
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Unauthorized (Unauthorized)"), TARGET_IDP).getCode(), "unauthorized");
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: null (null)"),
        TARGET_IDP);
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: null (vendor:denied)"), TARGET_IDP).getCode(), "vendor:denied");
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: null (Bad credentials)"), TARGET_IDP).getCode(), "bad_credentials");
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: null (x(y))"),
        TARGET_IDP);
    assertEquals(assertAuth(new PulsarClientException.AuthenticationException(
            "Unable to obtain an access token: Denied (see docs) (invalid_grant)"),
        TARGET_IDP).getCode(), "invalid_grant");
    assertAuth(new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
        + "java.lang.SecurityException: Invalid client credentials (invalid_client)"),
        TARGET_IDP);
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

  @Test(timeOut = 10_000)
  public void aLongTokenErrorIsReadInLinearTime() {
    assertSourceRead(new PulsarClientException.AuthenticationException(
        "Unable to obtain an access token: " + " (x".repeat(20_000)), TARGET_IDP);
  }

  @Test
  public void clientSideFailures() {
    assertAuth(new PulsarClientException.AuthenticationException("Unable to read private key: no such file"),
        null);
    assertAuth(new PulsarClientException.UnsupportedAuthenticationException("bad auth plugin"),
        null);
    assertSourceRead(new PulsarClientException.GettingAuthenticationDataException(
        new ConnectException("Connection refused")), TARGET_IDP);
    assertSourceRead(new PulsarClientException.InvalidConfigurationException("bad config"), null);
    assertSourceRead(new PulsarClientException.InvalidServiceURL("Invalid service URL", null), null);
    assertSourceRead(new PulsarClientException.InvalidTopicNameException("bad topic"), null);
    assertSourceRead(new PulsarClientException.TooManyRequestsException("Requests number out of config: 50000"),
        null);
    assertNotClassified(new PulsarClientException.AlreadyClosedException("Client already closed"));
  }

  @Test
  public void topicPermissionDenied() {
    assertAuth(underPinot(new PulsarClientException.AuthorizationException(
            "Client is not authorized to Get Partition Metadata")), TARGET_BROKER);
  }

  @Test
  public void brokerCannotServeUs() {
    assertSourceRead(underPinot(new PulsarClientException.TopicDoesNotExistException("Topic not found")),
        TARGET_BROKER);
    assertSourceRead(new PulsarClientException.ServiceNotReadyException("bundle unloading"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.TooManyRequestsException("lookup throttled"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.TimeoutException("could not get the last message id"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.NotFoundException("Not found: lookup"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException.ConnectException("Disconnected from server at broker:6650"),
        TARGET_BROKER);
    assertSourceRead(new PulsarClientException.ConsumerBusyException("Exclusive consumer is already connected"),
        TARGET_BROKER);
    assertSourceRead(new PulsarClientException.NotAllowedException("Subscription type not allowed"), TARGET_BROKER);
    assertSourceRead(new PulsarClientException("Connection already closed"), TARGET_BROKER);
  }

  @Test
  public void baseClassAloneDefersToItsCause() {
    Throwable connect = new PulsarClientException(new ConnectException("Connection refused"));
    assertEquals(classify(connect).getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
    assertNotClassified(new PulsarClientException("HTTP get request failed: Forbidden"));
  }

  @Test
  public void adminStatusTellsAuthenticationFromAuthorization() {
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Unauthorized", 401),
        TARGET_ADMIN);
    assertAuth(new PulsarAdminException.NotAuthorizedException(null, "Forbidden", 403),
        TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException.NotFoundException(null, "Schema not found", 404), TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException.ServerSideErrorException(null, "unavailable", "", 503), TARGET_ADMIN);
    assertSourceRead(new PulsarAdminException(null, "Request Timeout", 408), TARGET_ADMIN);
    assertNotClassified(new PulsarAdminException(new IllegalStateException("Client instance has been closed.")));
  }
}
