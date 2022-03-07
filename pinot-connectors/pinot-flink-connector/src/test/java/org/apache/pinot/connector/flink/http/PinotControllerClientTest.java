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
package org.apache.pinot.connector.flink.http;

import com.google.common.collect.Lists;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PinotControllerClientTest {

  private static final String TABLE_NAME = "demand";

  @Test
  public void testGetPinotControllerInstancesFromController()
      throws Exception {
    Client client = mock(Client.class);
    WebTarget target = mock(WebTarget.class);
    Invocation.Builder builder = mock(Invocation.Builder.class);
    Response response = mock(Response.class);
    MultivaluedMap<String, Object> headers = getRequestHeadersToPinot();
    String expectedFullURL = "http://localhost:9000/instances";
    final Map<String, Object> resEntity =
        JsonUtils.stringToObject(fixture("fixtures/pinotControllerInstances.json"), Map.class);

    when(client.target(expectedFullURL)).thenReturn(target);
    when(target.request()).thenReturn(builder);
    when(builder.headers(headers)).thenReturn(builder);
    when(builder.get()).thenReturn(response);
    when(response.getStatus()).thenReturn(200);
    when(response.getEntity()).thenReturn(resEntity);
    when(response.readEntity(Map.class)).thenReturn(resEntity);

    PinotControllerClient controllerClient = new PinotControllerClient();
    controllerClient.setHttpClient(client);
    List<String> instances = controllerClient.getControllerInstances(new MultivaluedHashMap<>());
    assertEquals(Lists.newArrayList("pinot-prod02:5983"), instances);

    verify(client, times(1)).target(expectedFullURL);
    verify(target, times(1)).request();
    verify(builder, times(1)).headers(headers);
    verify(builder, times(1)).get();
  }

  @Test
  public void testGetPinotSchemaStrFromController()
      throws Exception {
    Client client = mock(Client.class);
    WebTarget target = mock(WebTarget.class);
    Invocation.Builder builder = mock(Invocation.Builder.class);
    Response response = mock(Response.class);
    MultivaluedMap<String, Object> headers = getRequestHeadersToPinot();
    String expectedFullURL = "http://localhost:9000/schemas/demand";
    final Map<String, Object> resEntity =
        JsonUtils.stringToObject(fixture("fixtures/pinotTableSchema.json"), Map.class);

    when(client.target(expectedFullURL)).thenReturn(target);
    when(target.request()).thenReturn(builder);
    when(builder.headers(headers)).thenReturn(builder);
    when(builder.get()).thenReturn(response);
    when(response.getStatus()).thenReturn(200);
    when(response.getEntity()).thenReturn(resEntity);
    when(response.readEntity(Map.class)).thenReturn(resEntity);

    PinotControllerClient controllerClient = new PinotControllerClient();
    controllerClient.setHttpClient(client);
    String schemaStrFromController =
        controllerClient.getSchemaStrFromController(TABLE_NAME, new MultivaluedHashMap<>());
    assertEquals(
        JsonUtils.objectToString(JsonUtils.stringToObject(fixture("fixtures/pinotTableSchema.json"), Map.class)),
        schemaStrFromController);

    verify(client, times(1)).target(expectedFullURL);
    verify(target, times(1)).request();
    verify(builder, times(1)).headers(headers);
    verify(builder, times(1)).get();
  }

  @Test
  public void testGetPinotConfigStrFromController()
      throws Exception {
    Client client = mock(Client.class);
    WebTarget target = mock(WebTarget.class);
    Invocation.Builder builder = mock(Invocation.Builder.class);
    Response response = mock(Response.class);
    MultivaluedMap<String, Object> headers = getRequestHeadersToPinot();
    String expectedFullURL = "http://localhost:9000/tables/demand?type=realtime";
    final Map<String, Object> resEntity = new HashMap<>();
    resEntity.put(PinotControllerClient.TableType.REALTIME.toString().toUpperCase(),
        JsonUtils.stringToObject(fixture("fixtures/pinotTableConfigLowLevel.json"), TableConfig.class));

    when(client.target(expectedFullURL)).thenReturn(target);
    when(target.request()).thenReturn(builder);
    when(builder.headers(headers)).thenReturn(builder);
    when(builder.get()).thenReturn(response);
    when(response.getStatus()).thenReturn(200);
    when(response.getEntity()).thenReturn(resEntity);
    when(response.readEntity(Map.class)).thenReturn(resEntity);

    PinotControllerClient controllerClient = new PinotControllerClient();
    controllerClient.setHttpClient(client);
    String configStrFromController =
        controllerClient.getPinotConfigStrFromController(TABLE_NAME, PinotControllerClient.TableType.REALTIME,
            new MultivaluedHashMap<>());
    assertEquals(JsonUtils.objectToString(
            JsonUtils.stringToObject(fixture("fixtures/pinotTableConfigLowLevel.json"), TableConfig.class)),
        configStrFromController);

    verify(client, times(1)).target(expectedFullURL);
    verify(target, times(1)).request();
    verify(builder, times(1)).headers(headers);
    verify(builder, times(1)).get();
  }

  public static MultivaluedMap<String, Object> getRequestHeadersToPinot() {
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.putSingle(PinotControllerClient.HEADER_CONTENT_TYPE, MediaType.APPLICATION_JSON_TYPE);
    return headers;
  }

  private static String fixture(String filename)
      throws Exception {
    ClassLoader classLoader = PinotControllerClientTest.class.getClassLoader();
    final URL resource = classLoader.getResource(filename);
    try (InputStream inputStream = resource.openStream()) {
      return IOUtils.toString(inputStream).trim();
    }
  }
}
