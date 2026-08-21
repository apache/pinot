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
package org.apache.pinot.controller.api.resources;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class ManualAuthorizationValidationOrderTest {
  private static final String TABLE_NAME = "unauthorizedGroovy";

  @Test
  public void schemaJsonValidationAuthorizesBeforeSemanticValidation() {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    PinotSchemaRestletResource resource = new PinotSchemaRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = groovyEnabledControllerConf();
    resource._accessControlFactory = denyingAccess(AccessType.READ, Actions.Table.VALIDATE_SCHEMA);

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.validateSchema(invalidGroovySchema().toSingleLineJsonString(), mock(HttpHeaders.class),
            request("/schemas/validate")));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    verifyNoInteractions(resourceManager);
  }

  @Test
  public void schemaMultipartValidationAuthorizesBeforeSemanticValidation() {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    PinotSchemaRestletResource resource = new PinotSchemaRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = groovyEnabledControllerConf();
    resource._accessControlFactory = denyingAccess(AccessType.READ, Actions.Table.VALIDATE_SCHEMA);

    FormDataBodyPart bodyPart = mock(FormDataBodyPart.class);
    when(bodyPart.getValueAs(InputStream.class)).thenReturn(
        new ByteArrayInputStream(invalidGroovySchema().toSingleLineJsonString().getBytes(StandardCharsets.UTF_8)));
    FormDataMultiPart multiPart = mock(FormDataMultiPart.class);
    when(multiPart.getFields()).thenReturn(Map.of("schema", List.of(bodyPart)));

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.validateSchema(multiPart, mock(HttpHeaders.class), request("/schemas/validate")));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    verifyNoInteractions(resourceManager);
  }

  @Test
  public void tableConfigsCreateAuthorizesBeforeSemanticValidation()
      throws Exception {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = groovyEnabledControllerConf();
    resource._accessControlFactory = denyingAccess(AccessType.CREATE, Actions.Table.CREATE_TABLE);

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.addConfig(invalidGroovyTableConfigs().toJsonString(), null, false, mock(HttpHeaders.class),
            request("/tableConfigs")));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    verify(resourceManager).hasOfflineTable(TABLE_NAME);
    verify(resourceManager).hasRealtimeTable(TABLE_NAME);
    verify(resourceManager).getSchema(TABLE_NAME);
    verifyNoMoreInteractions(resourceManager);
  }

  @Test
  public void tableConfigsValidationAuthorizesBeforeSemanticValidation() {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = groovyEnabledControllerConf();
    resource._accessControlFactory = denyingAccess(AccessType.READ, Actions.Table.VALIDATE_TABLE_CONFIGS);

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.validateConfig(invalidGroovyTableConfigs().toJsonString(), null, mock(HttpHeaders.class),
            request("/tableConfigs/validate")));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    verifyNoInteractions(resourceManager);
  }

  @Test
  public void tableConfigsTuneAuthorizesBeforeSemanticValidation() {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = groovyEnabledControllerConf();
    resource._accessControlFactory = denyingAccess(AccessType.READ, Actions.Table.VALIDATE_TABLE_CONFIGS);

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.tuneConfig(invalidGroovyTableConfigs().toJsonString(), null, mock(HttpHeaders.class),
            request("/tableConfigs/tune")));

    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    verifyNoInteractions(resourceManager);
  }

  private static Schema invalidGroovySchema() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("source", DataType.STRING)
        .addSingleValueDimension("derived", DataType.STRING)
        .build();
    schema.getFieldSpecFor("derived").setTransformFunction("Groovy({ def invalid = }, source)");
    return schema;
  }

  private static TableConfigs invalidGroovyTableConfigs() {
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    return new TableConfigs(TABLE_NAME, invalidGroovySchema(), offlineTableConfig, null);
  }

  private static Request request(String path) {
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost" + path));
    return request;
  }

  private static ControllerConf groovyEnabledControllerConf() {
    return new ControllerConf(Map.of(ControllerConf.DISABLE_GROOVY, false));
  }

  private static AccessControlFactory denyingAccess(AccessType expectedAccessType, String expectedAction) {
    return () -> new AccessControl() {
      @Override
      public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
        assertEquals(tableName, TABLE_NAME);
        assertEquals(accessType, expectedAccessType);
        return true;
      }

      @Override
      public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
        assertEquals(targetType, TargetType.TABLE);
        assertEquals(targetId, TABLE_NAME);
        assertEquals(action, expectedAction);
        return false;
      }
    };
  }
}
