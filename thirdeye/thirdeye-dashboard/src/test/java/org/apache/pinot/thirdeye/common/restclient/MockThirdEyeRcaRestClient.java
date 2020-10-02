/*
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

package org.apache.pinot.thirdeye.common.restclient;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;


public class MockThirdEyeRcaRestClient {

  public static ThirdEyeRcaRestClient setupMockClient(final Map<String, Object> expectedResponse) {
    Client client = mock(Client.class);

    WebTarget webTarget = mock(WebTarget.class);
    when(client.target(anyString())).thenReturn(webTarget);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(webTarget.request(anyString())).thenReturn(builder);
    when(builder.headers(any())).thenReturn(builder);

    Response response = mock(Response.class);
    when(builder.get()).thenReturn(response);
    when(response.readEntity(any(GenericType.class))).thenReturn(expectedResponse);

    ThirdEyePrincipal principal = new ThirdEyePrincipal(null, "dummy");

    return new ThirdEyeRcaRestClient(client, principal);
  }
}
