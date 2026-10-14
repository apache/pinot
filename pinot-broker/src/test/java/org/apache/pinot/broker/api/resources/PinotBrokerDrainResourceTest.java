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
package org.apache.pinot.broker.api.resources;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the HTTP contract of the broker drain resource.
public class PinotBrokerDrainResourceTest {
  @Test
  public void testDrainWithoutCoordinationReturnsNotImplemented()
      throws ReflectiveOperationException {
    PinotBrokerDrainResource drainResource = createDrainResource(BrokerDrainManager.unsupported("unknown"));

    WebApplicationException exception =
        expectThrows(WebApplicationException.class, () -> drainResource.drain(0L, false));

    assertEquals(exception.getResponse().getStatus(), Response.Status.NOT_IMPLEMENTED.getStatusCode());
    assertEquals(exception.getMessage(),
        "Broker drain is unavailable because this broker was started without drain coordination");
  }

  @Test
  public void testDrainDuringStartupReturnsServiceUnavailable()
      throws ReflectiveOperationException {
    BrokerDrainManager drainManager = new BrokerDrainManager("Broker_localhost_8099", () -> null, () -> {
    }, () -> {
    }, 10_000L);
    PinotBrokerDrainResource drainResource = createDrainResource(drainManager);

    WebApplicationException exception =
        expectThrows(WebApplicationException.class, () -> drainResource.drain(0L, false));

    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertEquals(exception.getMessage(), "Broker Broker_localhost_8099 is still starting and cannot drain yet");
  }

  @Test
  public void testInvalidTimeoutReturnsBadRequest()
      throws ReflectiveOperationException {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 10_000L);
    PinotBrokerDrainResource drainResource = createDrainResource(drainManager);

    WebApplicationException exception =
        expectThrows(WebApplicationException.class, () -> drainResource.drain(-2L, false));

    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertFalse(drainManager.isDraining());
  }

  @Test
  public void testDrainTimeoutWithInFlightQueryReturnsRequestTimeout()
      throws ReflectiveOperationException {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 10_000L);
    PinotBrokerDrainResource drainResource = createDrainResource(drainManager);

    try (BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery()) {
      assertNotNull(queryPermit);
      Response response = drainResource.drain(0L, false);

      assertEquals(response.getStatus(), Response.Status.REQUEST_TIMEOUT.getStatusCode());
      BrokerDrainManager.DrainStatus status = (BrokerDrainManager.DrainStatus) response.getEntity();
      // The drain stays in effect after the timeout: new queries are rejected while the accepted one finishes
      assertTrue(status.isDraining());
      assertFalse(status.isDrained());
      assertEquals(status.getInFlightQueries(), 1);
      assertNull(drainManager.tryAcquireQuery());
    }
    assertTrue(drainManager.getStatus().isDrained());
  }

  @Test
  public void testShutdownRunsAfterResponseCompletion()
      throws Exception {
    CountDownLatch shutdown = new CountDownLatch(1);
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, shutdown::countDown, 10_000L);
    PinotBrokerDrainResource drainResource = createDrainResource(drainManager);
    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
    when(asyncResponse.resume(any(Response.class))).thenReturn(true);

    drainResource.drainAsync(asyncResponse, 0L, true, mockRequest());

    assertFalse(drainManager.getStatus().isShutdownTriggered());
    ArgumentCaptor<CompletionCallback> completionCallback = ArgumentCaptor.forClass(CompletionCallback.class);
    verify(asyncResponse).register(completionCallback.capture());
    verify(asyncResponse).resume(any(Response.class));

    completionCallback.getValue().onComplete(null);
    assertTrue(shutdown.await(10, TimeUnit.SECONDS));
    assertTrue(drainManager.getStatus().isShutdownTriggered());
  }

  @Test
  public void testDrainWithoutAdminAccessIsForbidden()
      throws ReflectiveOperationException {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 10_000L);
    AccessControl accessControl = Mockito.mock(AccessControl.class);
    when(accessControl.authorizeAdminAccess(any())).thenReturn(new BasicAuthorizationResultImpl(false, "not admin"));
    AccessControlFactory accessControlFactory = Mockito.mock(AccessControlFactory.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    PinotBrokerDrainResource drainResource = createDrainResource(drainManager, accessControlFactory);
    AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);

    WebApplicationException exception = expectThrows(WebApplicationException.class,
        () -> drainResource.drainAsync(asyncResponse, 0L, true, mockRequest()));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    assertEquals(exception.getMessage(),
        "Broker administrative access is required to drain the broker. Reason: not admin");
    assertFalse(drainManager.isDraining());
    assertTrue(drainManager.getStatus().isAcceptingQueries());
    verify(asyncResponse, never()).resume(any(Response.class));
  }

  private PinotBrokerDrainResource createDrainResource(BrokerDrainManager drainManager)
      throws ReflectiveOperationException {
    AccessControlFactory accessControlFactory = new AllowAllAccessControlFactory();
    return createDrainResource(drainManager, accessControlFactory);
  }

  private PinotBrokerDrainResource createDrainResource(BrokerDrainManager drainManager,
      AccessControlFactory accessControlFactory)
      throws ReflectiveOperationException {
    PinotBrokerDrainResource drainResource = new PinotBrokerDrainResource();
    Field drainManagerField = PinotBrokerDrainResource.class.getDeclaredField("_brokerDrainManager");
    drainManagerField.setAccessible(true);
    drainManagerField.set(drainResource, drainManager);
    Field accessControlFactoryField = PinotBrokerDrainResource.class.getDeclaredField("_accessControlFactory");
    accessControlFactoryField.setAccessible(true);
    accessControlFactoryField.set(drainResource, accessControlFactory);
    return drainResource;
  }

  private static Request mockRequest() {
    Request request = Mockito.mock(Request.class);
    when(request.getHeaderNames()).thenReturn(List.of());
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost:8099/drain"));
    return request;
  }
}
