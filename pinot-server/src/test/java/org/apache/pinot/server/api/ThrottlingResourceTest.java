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
package org.apache.pinot.server.api;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ThrottlingResourceTest extends BaseResourceTest {

  @Test
  public void testStateAndSetLimit() {
    Response r = _webTarget.path("/throttling/state").request().get(Response.class);
    assertEquals(r.getStatus(), 200);
    String json = r.readEntity(String.class);
    assertTrue(json.contains("currentConcurrencyLimit"));

    String body = "{\"_limit\":2}";
    Response r2 =
        _webTarget.path("/throttling/setLimit").request().post(Entity.entity(body, MediaType.APPLICATION_JSON));
    assertEquals(r2.getStatus(), 200);
    String json2 = r2.readEntity(String.class);
    assertTrue(json2.contains("currentConcurrencyLimit"));

    String bad = "{\"_limit\":0}";
    Response r3 =
        _webTarget.path("/throttling/setLimit").request().post(Entity.entity(bad, MediaType.APPLICATION_JSON));
    assertEquals(r3.getStatus(), 400);
  }
}
