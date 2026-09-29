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
package org.apache.pinot.client.admin;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.asynchttpclient.Response;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class PinotAdminTransportTest {
  private PinotAdminTransport _transport;

  @BeforeMethod
  public void setUp() {
    _transport = new PinotAdminTransport(new Properties(), null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _transport.close();
  }

  @Test
  public void testParseBinaryResponseReturnsBodyOnSuccess()
      throws Exception {
    Response response = Mockito.mock(Response.class);
    byte[] body = "segment-bytes".getBytes(StandardCharsets.UTF_8);
    Mockito.when(response.getStatusCode()).thenReturn(200);
    Mockito.when(response.getResponseBodyAsBytes()).thenReturn(body);

    assertEquals(_transport.parseBinaryResponse(response), body);
  }

  @Test
  public void testParseBinaryResponseThrowsOnAuthenticationFailure() {
    Response response = Mockito.mock(Response.class);
    Mockito.when(response.getStatusCode()).thenReturn(401);
    Mockito.when(response.getResponseBody()).thenReturn("unauthorized");

    PinotAdminAuthenticationException exception =
        expectThrows(PinotAdminAuthenticationException.class, () -> _transport.parseBinaryResponse(response));
    assertEquals(exception.getMessage(), "Authentication failed: unauthorized");
  }
}
