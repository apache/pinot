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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.TableConfigVersionMismatchException;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.ConfigValidationException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;


/// Verifies that table-config REST resources redact read responses and restore unchanged markers on update.
public class TableConfigRedactionResourceTest {
  private static final String RAW_TABLE_NAME = "credentialTable";
  private static final String OFFLINE_TABLE_NAME = RAW_TABLE_NAME + "_OFFLINE";
  private static final String REALTIME_TABLE_NAME = RAW_TABLE_NAME + "_REALTIME";
  private static final String LITERAL_PASSWORD = "literal-password";
  private static final String PASSWORD_PLACEHOLDER = "${PINOT_TEST_PASSWORD}";
  private static final String ORIGINAL_CLIENT_ID = "original-client";
  private static final String EDITED_CLIENT_ID = "edited-client";
  private static final String LITERAL_PASSWORD_KEY = "literal.password";
  private static final String PLACEHOLDER_PASSWORD_KEY = "placeholder.password";
  private static final String CLIENT_ID_KEY = "client.id";
  private static final int OFFLINE_CONFIG_VERSION = 7;
  private static final int REALTIME_CONFIG_VERSION = 11;
  private static final int SCHEMA_VERSION = 313;

  @Test
  public void testGetTableConfigRedactsLiteralAndPreservesPlaceholder()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;

    JsonNode response = JsonUtils.stringToJsonNode(resource.listTableConfigs(
        RAW_TABLE_NAME, TableType.OFFLINE.name(), headersWithoutDatabase()));
    JsonNode customConfigs = response.path(RedactedTableConfigResponse.CONFIGS_KEY).path(TableType.OFFLINE.name())
        .path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs");

    assertEquals(customConfigs.path(LITERAL_PASSWORD_KEY).asText(),
        TableConfigRedactionUtils.REDACTION_MARKER);
    assertEquals(customConfigs.path(PLACEHOLDER_PASSWORD_KEY).asText(), PASSWORD_PLACEHOLDER);
    assertEquals(customConfigs.path(CLIENT_ID_KEY).asText(), ORIGINAL_CLIENT_ID);
    assertEquals(response.path(RedactedTableConfigResponse.BASE_VERSIONS_KEY)
        .path(TableType.OFFLINE.name()).asInt(), OFFLINE_CONFIG_VERSION);
    assertFalse(response.toString().contains(LITERAL_PASSWORD));
    assertEquals(stored.getCustomConfig().getCustomConfigs().get(LITERAL_PASSWORD_KEY), LITERAL_PASSWORD,
        "Redacting a response must not mutate the stored config instance");
    verify(resourceManager).getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false);
  }

  @Test
  public void testGetCombinedTableConfigsRedactsLiteralAndPreservesPlaceholder()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;

    JsonNode response = JsonUtils.stringToJsonNode(resource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase()));
    JsonNode customConfigs = response.path(RedactedTableConfigsResponse.CONFIGS_KEY).path("offline")
        .path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs");

    assertEquals(customConfigs.path(LITERAL_PASSWORD_KEY).asText(),
        TableConfigRedactionUtils.REDACTION_MARKER);
    assertEquals(customConfigs.path(PLACEHOLDER_PASSWORD_KEY).asText(), PASSWORD_PLACEHOLDER);
    assertEquals(customConfigs.path(CLIENT_ID_KEY).asText(), ORIGINAL_CLIENT_ID);
    assertEquals(response.path(RedactedTableConfigsResponse.BASE_VERSIONS_KEY)
        .path("offline").asInt(), OFFLINE_CONFIG_VERSION);
    assertFalse(response.toString().contains(LITERAL_PASSWORD));
    verify(resourceManager).getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false);
  }

  @Test
  public void testResponseEnvelopesCannotBeDeserializedAsLegacyUpdateBodies()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);

    PinotTableRestletResource tableResource = new PinotTableRestletResource();
    tableResource._pinotHelixResourceManager = resourceManager;
    TableConfigsRestletResource combinedResource = new TableConfigsRestletResource();
    combinedResource._pinotHelixResourceManager = resourceManager;

    String tableResponse = tableResource.listTableConfigs(
        RAW_TABLE_NAME, TableType.OFFLINE.name(), headersWithoutDatabase());
    String combinedResponse = combinedResource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase());

    expectThrows(MismatchedInputException.class,
        () -> JsonUtils.stringToObject(tableResponse, TableConfig.class));
    expectThrows(MismatchedInputException.class,
        () -> JsonUtils.stringToObject(combinedResponse, TableConfigs.class));

    JsonNode responseNode = JsonUtils.stringToJsonNode(tableResponse);
    JsonNode unwrapped = PinotTableRestletResource.unwrapTableConfigResponse(responseNode);
    assertEquals(unwrapped.path(TableType.OFFLINE.name()),
        responseNode.path(RedactedTableConfigResponse.CONFIGS_KEY).path(TableType.OFFLINE.name()));

    ObjectNode legacyResponse = JsonUtils.newObjectNode();
    legacyResponse.set(TableType.OFFLINE.name(), TableConfigRedactionUtils.redact(stored).toJsonNode());
    expectThrows(IOException.class,
        () -> PinotTableRestletResource.unwrapTableConfigResponse(legacyResponse));
  }

  @Test
  public void testUpdateTableConfigRestoresMaskedStoredCredential()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    String getResponse = resource.listTableConfigs(
        RAW_TABLE_NAME, TableType.OFFLINE.name(), headersWithoutDatabase());
    String editedResponse = editTableConfigResponse(getResponse, TableType.OFFLINE);

    try (MockedStatic<TableConfigValidationUtils> ignored = Mockito.mockStatic(TableConfigValidationUtils.class)) {
      resource.updateTableConfig(RAW_TABLE_NAME, null, false, headersWithoutDatabase(), editedResponse);
    }

    verify(resourceManager).updateTableConfigsAtomicallyWithSchemaCheck(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> {
          assertRestoredCredentialAndEdit(configs.get(TableType.OFFLINE));
          return configs.size() == 1;
        }), eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION)), eq(false));
    verify(resourceManager, Mockito.times(2)).getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false);
  }

  @Test
  public void testUpdateCombinedTableConfigsRestoresMaskedStoredCredential()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    String getResponse = resource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase());
    String editedResponse = editTableConfigsResponse(getResponse, TableType.OFFLINE);

    try (MockedStatic<SchemaUtils> ignoredSchema = Mockito.mockStatic(SchemaUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class);
        MockedStatic<TaskConfigUtils> ignoredTask = Mockito.mockStatic(TaskConfigUtils.class);
        MockedStatic<TableConfigValidatorRegistry> ignoredRegistry =
            Mockito.mockStatic(TableConfigValidatorRegistry.class);
        MockedStatic<TableConfigTunerUtils> ignoredTuner = Mockito.mockStatic(TableConfigTunerUtils.class)) {
      resource.updateConfig(
          RAW_TABLE_NAME, null, false, false, editedResponse, headersWithoutDatabase());
    }

    verify(resourceManager).updateTableConfigsAtomically(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> {
          assertRestoredCredentialAndEdit(configs.get(TableType.OFFLINE));
          return configs.size() == 1;
        }), eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION)), eq(false), eq(false));
    verify(resourceManager, Mockito.times(2)).getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false);
  }

  @Test
  public void testBareExtractedMaskedBodiesAreRejected()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource tableResource = new PinotTableRestletResource();
    tableResource._pinotHelixResourceManager = resourceManager;
    String tableResponse = tableResource.listTableConfigs(
        RAW_TABLE_NAME, TableType.OFFLINE.name(), headersWithoutDatabase());
    String extractedTableConfig = JsonUtils.stringToJsonNode(tableResponse)
        .path(RedactedTableConfigResponse.CONFIGS_KEY).path(TableType.OFFLINE.name()).toString();

    ControllerApplicationException tableError = expectThrows(ControllerApplicationException.class,
        () -> tableResource.updateTableConfig(
            RAW_TABLE_NAME, null, false, headersWithoutDatabase(), extractedTableConfig));
    assertEquals(tableError.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    TableConfigsRestletResource combinedResource = new TableConfigsRestletResource();
    combinedResource._pinotHelixResourceManager = resourceManager;
    String combinedResponse = combinedResource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase());
    String extractedTableConfigs = JsonUtils.stringToJsonNode(combinedResponse)
        .path(RedactedTableConfigsResponse.CONFIGS_KEY).toString();
    ControllerApplicationException combinedError = expectThrows(ControllerApplicationException.class,
        () -> combinedResource.updateConfig(
            RAW_TABLE_NAME, null, false, false, extractedTableConfigs, headersWithoutDatabase()));
    assertEquals(combinedError.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verify(resourceManager, never()).updateTableConfig(Mockito.any(TableConfig.class), Mockito.anyInt(), eq(false));
  }

  @Test
  public void testLegacyLiteralUpdateBodiesRemainAccepted()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    TableConfig literalUpdate = tableConfigWithCredentials();
    literalUpdate.getCustomConfig().getCustomConfigs().put(CLIENT_ID_KEY, EDITED_CLIENT_ID);
    PinotHelixResourceManager tableResourceManager = mockResourceManager(stored);
    PinotTableRestletResource tableResource = new PinotTableRestletResource();
    tableResource._pinotHelixResourceManager = tableResourceManager;
    tableResource._controllerConf = mock(ControllerConf.class);
    tableResource._pinotTaskManager = mock(PinotTaskManager.class);
    try (MockedStatic<TableConfigValidationUtils> ignored = Mockito.mockStatic(TableConfigValidationUtils.class)) {
      tableResource.updateTableConfig(
          RAW_TABLE_NAME, null, false, headersWithoutDatabase(), literalUpdate.toJsonString());
    }
    verify(tableResourceManager).updateTableConfig(
        Mockito.any(TableConfig.class), eq(OFFLINE_CONFIG_VERSION), eq(false));

    PinotHelixResourceManager combinedResourceManager = mockResourceManager(stored);
    TableConfigsRestletResource combinedResource = new TableConfigsRestletResource();
    combinedResource._pinotHelixResourceManager = combinedResourceManager;
    combinedResource._controllerConf = mock(ControllerConf.class);
    combinedResource._pinotTaskManager = mock(PinotTaskManager.class);
    TableConfigs literalTableConfigs = new TableConfigs(RAW_TABLE_NAME, schema(), literalUpdate, null);
    try (MockedStatic<SchemaUtils> ignoredSchema = Mockito.mockStatic(SchemaUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class);
        MockedStatic<TaskConfigUtils> ignoredTask = Mockito.mockStatic(TaskConfigUtils.class);
        MockedStatic<TableConfigValidatorRegistry> ignoredRegistry =
            Mockito.mockStatic(TableConfigValidatorRegistry.class);
        MockedStatic<TableConfigTunerUtils> ignoredTuner = Mockito.mockStatic(TableConfigTunerUtils.class)) {
      combinedResource.updateConfig(RAW_TABLE_NAME, null, false, false, literalTableConfigs.toJsonString(),
          headersWithoutDatabase());
    }
    verify(combinedResourceManager).updateTableConfigsAtomically(
        Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> configs.size() == 1 && configs.containsKey(TableType.OFFLINE)),
        eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION)), eq(false), eq(false));
  }

  @Test
  public void testEnvelopeBaseVersionMustMatchGetSnapshot()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);

    ControllerApplicationException error = expectThrows(ControllerApplicationException.class,
        () -> resource.updateTableConfig(RAW_TABLE_NAME, null, false, headersWithoutDatabase(),
            tableConfigEnvelope(maskedEditedConfig(stored), OFFLINE_CONFIG_VERSION - 1)));

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    verify(resourceManager, never()).updateTableConfig(Mockito.any(TableConfig.class), Mockito.anyInt(), eq(false));
  }

  @Test
  public void testCombinedEnvelopeChecksSnapshotBeforeSchemaUpdate()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    TableConfigs submitted = new TableConfigs(RAW_TABLE_NAME, schema(), maskedEditedConfig(stored), null);

    ControllerApplicationException error = expectThrows(ControllerApplicationException.class,
        () -> resource.updateConfig(RAW_TABLE_NAME, null, false, false,
            tableConfigsEnvelope(submitted, Map.of("offline", OFFLINE_CONFIG_VERSION - 1)),
            headersWithoutDatabase()));

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    verify(resourceManager, never()).updateSchema(Mockito.any(Schema.class), eq(false), eq(false));
    verify(resourceManager, never()).updateTableConfig(Mockito.any(TableConfig.class), Mockito.anyInt(), eq(false));
  }

  @Test
  public void testEnvelopeRejectsMissingNegativeAndUnknownBaseVersions()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    ObjectNode response = (ObjectNode) JsonUtils.stringToJsonNode(resource.listTableConfigs(
        RAW_TABLE_NAME, TableType.OFFLINE.name(), headersWithoutDatabase()));

    ObjectNode missing = response.deepCopy();
    missing.remove(RedactedTableConfigResponse.BASE_VERSIONS_KEY);
    expectThrows(JsonMappingException.class,
        () -> JsonUtils.stringToObject(missing.toString(), RedactedTableConfigResponse.class));

    ObjectNode negative = response.deepCopy();
    ((ObjectNode) negative.path(RedactedTableConfigResponse.BASE_VERSIONS_KEY))
        .put(TableType.OFFLINE.name(), -1);
    expectThrows(JsonMappingException.class,
        () -> JsonUtils.stringToObject(negative.toString(), RedactedTableConfigResponse.class));

    ObjectNode unknown = response.deepCopy();
    ((ObjectNode) unknown.path(RedactedTableConfigResponse.BASE_VERSIONS_KEY))
        .put(TableType.REALTIME.name(), REALTIME_CONFIG_VERSION);
    expectThrows(JsonMappingException.class,
        () -> JsonUtils.stringToObject(unknown.toString(), RedactedTableConfigResponse.class));
  }

  @Test
  public void testRealtimeGetAndUpdateBranchesRedactAndRestore()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials(TableType.REALTIME);
    PinotHelixResourceManager resourceManager = mockRealtimeResourceManager(stored);
    PinotTableRestletResource tableResource = new PinotTableRestletResource();
    tableResource._pinotHelixResourceManager = resourceManager;
    tableResource._controllerConf = mock(ControllerConf.class);
    tableResource._pinotTaskManager = mock(PinotTaskManager.class);
    TableConfigsRestletResource combinedResource = new TableConfigsRestletResource();
    combinedResource._pinotHelixResourceManager = resourceManager;

    String tableResponse = tableResource.listTableConfigs(
        RAW_TABLE_NAME, TableType.REALTIME.name(), headersWithoutDatabase());
    String combinedResponse = combinedResource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase());
    assertFalse(tableResponse.contains(LITERAL_PASSWORD));
    assertFalse(combinedResponse.contains(LITERAL_PASSWORD));
    assertEquals(JsonUtils.stringToJsonNode(tableResponse).path(RedactedTableConfigResponse.CONFIGS_KEY)
        .path(TableType.REALTIME.name())
        .path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs").path(LITERAL_PASSWORD_KEY).asText(),
        TableConfigRedactionUtils.REDACTION_MARKER);
    assertEquals(JsonUtils.stringToJsonNode(combinedResponse).path(RedactedTableConfigsResponse.CONFIGS_KEY)
        .path("realtime")
        .path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs").path(LITERAL_PASSWORD_KEY).asText(),
        TableConfigRedactionUtils.REDACTION_MARKER);

    try (MockedStatic<TableConfigValidationUtils> ignored = Mockito.mockStatic(TableConfigValidationUtils.class)) {
      tableResource.updateTableConfig(RAW_TABLE_NAME, null, false, headersWithoutDatabase(),
          editTableConfigResponse(tableResponse, TableType.REALTIME));
    }

    verify(resourceManager).updateTableConfigsAtomicallyWithSchemaCheck(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> {
          assertRestoredCredentialAndEdit(configs.get(TableType.REALTIME));
          return configs.size() == 1;
        }), eq(Map.of(TableType.REALTIME, REALTIME_CONFIG_VERSION)), eq(false));
    verify(resourceManager, Mockito.atLeastOnce()).getTableConfigWithVersion(REALTIME_TABLE_NAME, false, false);

    combinedResource._controllerConf = mock(ControllerConf.class);
    combinedResource._pinotTaskManager = mock(PinotTaskManager.class);
    try (MockedStatic<SchemaUtils> ignoredSchema = Mockito.mockStatic(SchemaUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class);
        MockedStatic<TaskConfigUtils> ignoredTask = Mockito.mockStatic(TaskConfigUtils.class);
        MockedStatic<TableConfigValidatorRegistry> ignoredRegistry =
            Mockito.mockStatic(TableConfigValidatorRegistry.class);
        MockedStatic<TableConfigTunerUtils> ignoredTuner = Mockito.mockStatic(TableConfigTunerUtils.class)) {
      combinedResource.updateConfig(RAW_TABLE_NAME, null, false, false,
          editTableConfigsResponse(combinedResponse, TableType.REALTIME),
          headersWithoutDatabase());
    }
    verify(resourceManager).updateTableConfigsAtomically(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> {
          assertRestoredCredentialAndEdit(configs.get(TableType.REALTIME));
          return configs.size() == 1;
        }), eq(Map.of(TableType.REALTIME, REALTIME_CONFIG_VERSION)), eq(false), eq(false));
  }

  @Test
  public void testMaskedTableUpdateReturnsConflictForStaleVersion()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    Mockito.doThrow(new TableConfigVersionMismatchException()).when(resourceManager)
        .updateTableConfigsAtomicallyWithSchemaCheck(Mockito.any(Schema.class), eq(SCHEMA_VERSION), Mockito.anyMap(),
            Mockito.anyMap(), eq(false));
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    resource._controllerMetrics = mock(ControllerMetrics.class);

    ControllerApplicationException error;
    try (MockedStatic<TableConfigValidationUtils> ignored = Mockito.mockStatic(TableConfigValidationUtils.class)) {
      error = expectThrows(ControllerApplicationException.class,
          () -> resource.updateTableConfig(RAW_TABLE_NAME, null, false, headersWithoutDatabase(),
              tableConfigEnvelope(maskedEditedConfig(stored), OFFLINE_CONFIG_VERSION)));
    }

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    assertFalse(error.getMessage().contains(LITERAL_PASSWORD));
    verify(resourceManager).updateTableConfigsAtomicallyWithSchemaCheck(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.anyMap(), eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION)), eq(false));
    verify(resourceManager, never()).updateTableConfig(Mockito.any(TableConfig.class), eq(false));
  }

  @Test
  public void testMaskedCombinedUpdateReturnsConflictForStaleVersion()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    Mockito.doThrow(new TableConfigVersionMismatchException()).when(resourceManager)
        .updateTableConfigsAtomically(Mockito.any(Schema.class), eq(SCHEMA_VERSION), Mockito.anyMap(), Mockito.anyMap(),
            eq(false), eq(false));
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    resource._controllerMetrics = mock(ControllerMetrics.class);
    TableConfigs submittedConfigs = new TableConfigs(RAW_TABLE_NAME, schema(), maskedEditedConfig(stored), null);

    ControllerApplicationException error;
    try (MockedStatic<SchemaUtils> ignoredSchema = Mockito.mockStatic(SchemaUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class);
        MockedStatic<TaskConfigUtils> ignoredTask = Mockito.mockStatic(TaskConfigUtils.class);
        MockedStatic<TableConfigValidatorRegistry> ignoredRegistry =
            Mockito.mockStatic(TableConfigValidatorRegistry.class);
        MockedStatic<TableConfigTunerUtils> ignoredTuner = Mockito.mockStatic(TableConfigTunerUtils.class)) {
      error = expectThrows(ControllerApplicationException.class,
          () -> resource.updateConfig(RAW_TABLE_NAME, null, false, false,
              tableConfigsEnvelope(submittedConfigs, Map.of("offline", OFFLINE_CONFIG_VERSION)),
              headersWithoutDatabase()));
    }

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    assertFalse(error.getMessage().contains(LITERAL_PASSWORD));
    verify(resourceManager).updateTableConfigsAtomically(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.anyMap(), eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION)), eq(false), eq(false));
    verify(resourceManager, never()).updateTableConfig(Mockito.any(TableConfig.class), eq(false));
  }

  @Test
  public void testHybridTableEnvelopeRoundTripRestoresBothCredentials()
      throws Exception {
    TableConfig storedOffline = tableConfigWithCredentials(TableType.OFFLINE);
    TableConfig storedRealtime = tableConfigWithCredentials(TableType.REALTIME);
    PinotHelixResourceManager resourceManager = mockHybridResourceManager(storedOffline, storedRealtime);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);

    String response = resource.listTableConfigs(RAW_TABLE_NAME, null, headersWithoutDatabase());
    String editedResponse = editTableConfigResponse(
        editTableConfigResponse(response, TableType.OFFLINE), TableType.REALTIME);
    try (MockedStatic<TableConfigValidationUtils> ignored = Mockito.mockStatic(TableConfigValidationUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class)) {
      resource.updateTableConfig(RAW_TABLE_NAME, null, false, headersWithoutDatabase(), editedResponse);
    }

    verify(resourceManager).updateTableConfigsAtomicallyWithSchemaCheck(Mockito.any(Schema.class), eq(SCHEMA_VERSION),
        Mockito.argThat(configs -> {
          assertEquals(configs.size(), 2);
          assertRestoredCredentialAndEdit(configs.get(TableType.OFFLINE));
          assertRestoredCredentialAndEdit(configs.get(TableType.REALTIME));
          return true;
        }), eq(Map.of(TableType.OFFLINE, OFFLINE_CONFIG_VERSION,
            TableType.REALTIME, REALTIME_CONFIG_VERSION)), eq(false));
  }

  @Test
  public void testCombinedEnvelopeRejectsStaleSchemaWithoutMutation()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    ObjectNode response = (ObjectNode) JsonUtils.stringToJsonNode(
        resource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase()));
    ((ObjectNode) response.path(RedactedTableConfigsResponse.BASE_VERSIONS_KEY))
        .put(RedactedTableConfigsResponse.SCHEMA_VERSION_KEY, SCHEMA_VERSION - 1);

    ControllerApplicationException error = expectThrows(ControllerApplicationException.class,
        () -> resource.updateConfig(RAW_TABLE_NAME, null, false, false, response.toString(),
            headersWithoutDatabase()));

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    verify(resourceManager, never()).updateTableConfigsAtomically(Mockito.any(Schema.class), Mockito.anyInt(),
        Mockito.anyMap(), Mockito.anyMap(), eq(false), eq(false));
    verify(resourceManager, never()).updateSchema(Mockito.any(Schema.class), eq(false), eq(false));
  }

  @Test
  public void testVersionedEnvelopeRejectsMissingTypeAdditionWithoutMutation()
      throws Exception {
    TableConfig stored = tableConfigWithCredentials();
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    ObjectNode response = (ObjectNode) JsonUtils.stringToJsonNode(
        resource.getConfig(RAW_TABLE_NAME, headersWithoutDatabase()));
    ((ObjectNode) response.path(RedactedTableConfigsResponse.CONFIGS_KEY)).set("realtime",
        tableConfigWithCredentials(TableType.REALTIME).toJsonNode());

    ControllerApplicationException error;
    try (MockedStatic<SchemaUtils> ignoredSchema = Mockito.mockStatic(SchemaUtils.class);
        MockedStatic<TableConfigUtils> ignoredConfig = Mockito.mockStatic(TableConfigUtils.class);
        MockedStatic<TaskConfigUtils> ignoredTask = Mockito.mockStatic(TaskConfigUtils.class);
        MockedStatic<TableConfigValidatorRegistry> ignoredRegistry =
            Mockito.mockStatic(TableConfigValidatorRegistry.class)) {
      error = expectThrows(ControllerApplicationException.class,
          () -> resource.updateConfig(RAW_TABLE_NAME, null, false, false, response.toString(),
              headersWithoutDatabase()));
    }

    assertEquals(error.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    verify(resourceManager, never()).updateTableConfigsAtomically(Mockito.any(Schema.class), Mockito.anyInt(),
        Mockito.anyMap(), Mockito.anyMap(), Mockito.anyBoolean(), Mockito.anyBoolean());
    verify(resourceManager, never()).addTable(Mockito.any(TableConfig.class));
  }

  @Test
  public void testCopyCredentialOverridesOnlyReplaceRedactedFields()
      throws Exception {
    ObjectNode tableConfig = (ObjectNode) JsonUtils.stringToJsonNode("""
        {
          "ingestionConfig": {
            "streamIngestionConfig": {
              "streamConfigMaps": [
                {
                  "stream.kafka.consumer.prop.password": "*****",
                  "stream.kafka.topic.name": "*****",
                  "sasl.jaas.config": "example.LoginModule required username=alice password=*****;",
                  "schema.registry.url": "https://*****:*****@registry.example/path?token=*****&region=west"
                }
              ]
            }
          }
        }
        """);
    String credentialPointer = "/ingestionConfig/streamIngestionConfig/streamConfigMaps/0/"
        + "stream.kafka.consumer.prop.password";
    String jaasPointer = "/ingestionConfig/streamIngestionConfig/streamConfigMaps/0/sasl.jaas.config";
    String uriPointer = "/ingestionConfig/streamIngestionConfig/streamConfigMaps/0/schema.registry.url";
    String literalJaas = "example.LoginModule required username=alice password=literal-password;";
    String literalUri = "https://uri-user:uri-password@registry.example/path?token=literal-token&region=west";

    PinotTableRestletResource.applyCredentialOverrides(tableConfig, Map.of(
        credentialPointer, "replacement", jaasPointer, literalJaas, uriPointer, literalUri));

    assertEquals(tableConfig.at(credentialPointer).asText(), "replacement");
    assertEquals(tableConfig.at(jaasPointer).asText(), literalJaas);
    assertEquals(tableConfig.at(uriPointer).asText(), literalUri);
    expectThrows(IllegalArgumentException.class,
        () -> PinotTableRestletResource.applyCredentialOverrides(tableConfig,
            Map.of(credentialPointer, "second-replacement")));
    expectThrows(IllegalArgumentException.class,
        () -> PinotTableRestletResource.applyCredentialOverrides(tableConfig,
            Map.of("/ingestionConfig/streamIngestionConfig/streamConfigMaps/0/stream.kafka.topic.name", "other")));
  }

  @Test
  public void testAddTableRejectsCredentialMarkerBeforePersistence()
      throws Exception {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    TableConfig submitted = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(OFFLINE_TABLE_NAME)
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of(LITERAL_PASSWORD_KEY,
            TableConfigRedactionUtils.REDACTION_MARKER))))
        .build();

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.addTable(submitted.toJsonString(), null, false, headersWithoutDatabase(), null));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verify(resourceManager, never()).addTable(org.mockito.ArgumentMatchers.any(TableConfig.class));
  }

  @Test
  public void testAddCombinedTableConfigsRejectsCredentialMarkerBeforePersistence()
      throws Exception {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    TableConfigsRestletResource resource = new TableConfigsRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    TableConfig submitted = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(OFFLINE_TABLE_NAME)
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of(LITERAL_PASSWORD_KEY,
            TableConfigRedactionUtils.REDACTION_MARKER))))
        .build();
    TableConfigs submittedConfigs = new TableConfigs(RAW_TABLE_NAME, schema(), submitted, null);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.addConfig(submittedConfigs.toJsonString(), null, false, headersWithoutDatabase(), null));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verify(resourceManager, never()).addTable(org.mockito.ArgumentMatchers.any(TableConfig.class));
  }

  @Test
  public void testTaskCleanupPersistsUnresolvedCredentialPlaceholder()
      throws Exception {
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(LITERAL_PASSWORD_KEY, PASSWORD_PLACEHOLDER);
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(REALTIME_TABLE_NAME)
        .setCustomConfig(new TableCustomConfig(customConfigs))
        .setTaskConfig(new TableTaskConfig(Map.of(
            "TestTask", new HashMap<>(Map.of(PinotTaskManager.SCHEDULE_KEY, "0 0 * * * ?")))))
        .build();
    PinotHelixResourceManager resourceManager = mockRealtimeResourceManager(stored);
    PinotHelixTaskResourceManager taskResourceManager = mock(PinotHelixTaskResourceManager.class);
    when(taskResourceManager.getTaskStatesByTable("TestTask", REALTIME_TABLE_NAME)).thenReturn(Map.of());

    PinotTableRestletResource.tableTasksCleanup(
        REALTIME_TABLE_NAME, false, resourceManager, taskResourceManager);

    ArgumentCaptor<TableConfig> updatedConfig = ArgumentCaptor.forClass(TableConfig.class);
    verify(resourceManager).updateTableConfig(updatedConfig.capture());
    verify(resourceManager).getRealtimeTableConfig(RAW_TABLE_NAME, false, false);
    verify(resourceManager, never()).getTableConfig(REALTIME_TABLE_NAME);
    assertEquals(updatedConfig.getValue().getCustomConfig().getCustomConfigs().get(LITERAL_PASSWORD_KEY),
        PASSWORD_PLACEHOLDER);
    assertFalse(updatedConfig.getValue().getTaskConfig().getConfigsForTaskType("TestTask")
        .containsKey(PinotTaskManager.SCHEDULE_KEY));
  }

  @Test
  public void testUpdateValidationDiagnosticDoesNotExposeCredential()
      throws Exception {
    String escapedCredential = "p\"ass\\word\nline";
    TableConfig stored = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(OFFLINE_TABLE_NAME)
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of("provider.password", escapedCredential))))
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    PinotHelixResourceManager resourceManager = mockResourceManager(stored);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    resource._controllerConf = mock(ControllerConf.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    ConfigValidationException validationException =
        new ConfigValidationException("Invalid config " + stored.toJsonString());

    ControllerApplicationException error;
    try (MockedStatic<TableConfigValidationUtils> validation =
        Mockito.mockStatic(TableConfigValidationUtils.class)) {
      validation.when(() -> TableConfigValidationUtils.validateTableConfig(
              Mockito.any(TableConfig.class), Mockito.any(Schema.class), Mockito.nullable(String.class),
              Mockito.any(PinotHelixResourceManager.class), Mockito.any(ControllerConf.class),
              Mockito.any(PinotTaskManager.class), Mockito.any(TableConfig.class), eq(true)))
          .thenThrow(validationException);
      error = expectThrows(ControllerApplicationException.class,
          () -> resource.updateTableConfig(
              RAW_TABLE_NAME, null, false, headersWithoutDatabase(),
              tableConfigEnvelope(submitted, OFFLINE_CONFIG_VERSION)));
    }

    assertFalse(error.getMessage().contains(escapedCredential), error.getMessage());
    assertFalse(error.getMessage().contains("p\\\"ass\\\\word\\nline"), error.getMessage());
  }

  private static PinotHelixResourceManager mockResourceManager(TableConfig stored) {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.hasOfflineTable(RAW_TABLE_NAME)).thenReturn(true);
    when(resourceManager.hasTable(OFFLINE_TABLE_NAME)).thenReturn(true);
    when(resourceManager.getOfflineTableConfig(anyString(), eq(false), eq(false))).thenReturn(stored);
    when(resourceManager.getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false))
        .thenReturn(Pair.of(stored, OFFLINE_CONFIG_VERSION));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(stored);
    when(resourceManager.getTableSchema(anyString())).thenReturn(schema());
    when(resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(schema());
    when(resourceManager.getSchemaWithVersion(RAW_TABLE_NAME)).thenReturn(Pair.of(schema(), SCHEMA_VERSION));
    return resourceManager;
  }

  private static PinotHelixResourceManager mockRealtimeResourceManager(TableConfig stored) {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.hasRealtimeTable(RAW_TABLE_NAME)).thenReturn(true);
    when(resourceManager.hasTable(REALTIME_TABLE_NAME)).thenReturn(true);
    when(resourceManager.getRealtimeTableConfig(anyString(), eq(false), eq(false))).thenReturn(stored);
    when(resourceManager.getTableConfigWithVersion(REALTIME_TABLE_NAME, false, false))
        .thenReturn(Pair.of(stored, REALTIME_CONFIG_VERSION));
    when(resourceManager.getTableSchema(anyString())).thenReturn(schema());
    when(resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(schema());
    when(resourceManager.getSchemaWithVersion(RAW_TABLE_NAME)).thenReturn(Pair.of(schema(), SCHEMA_VERSION));
    return resourceManager;
  }

  private static PinotHelixResourceManager mockHybridResourceManager(TableConfig storedOffline,
      TableConfig storedRealtime) {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.hasOfflineTable(RAW_TABLE_NAME)).thenReturn(true);
    when(resourceManager.hasRealtimeTable(RAW_TABLE_NAME)).thenReturn(true);
    when(resourceManager.hasTable(OFFLINE_TABLE_NAME)).thenReturn(true);
    when(resourceManager.hasTable(REALTIME_TABLE_NAME)).thenReturn(true);
    when(resourceManager.getTableConfigWithVersion(OFFLINE_TABLE_NAME, false, false))
        .thenReturn(Pair.of(storedOffline, OFFLINE_CONFIG_VERSION));
    when(resourceManager.getTableConfigWithVersion(REALTIME_TABLE_NAME, false, false))
        .thenReturn(Pair.of(storedRealtime, REALTIME_CONFIG_VERSION));
    when(resourceManager.getSchemaWithVersion(RAW_TABLE_NAME)).thenReturn(Pair.of(schema(), SCHEMA_VERSION));
    return resourceManager;
  }

  private static TableConfig tableConfigWithCredentials() {
    return tableConfigWithCredentials(TableType.OFFLINE);
  }

  private static TableConfig tableConfigWithCredentials(TableType tableType) {
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(LITERAL_PASSWORD_KEY, LITERAL_PASSWORD);
    customConfigs.put(PLACEHOLDER_PASSWORD_KEY, PASSWORD_PLACEHOLDER);
    customConfigs.put(CLIENT_ID_KEY, ORIGINAL_CLIENT_ID);
    return new TableConfigBuilder(tableType)
        .setTableName(tableType == TableType.OFFLINE ? OFFLINE_TABLE_NAME : REALTIME_TABLE_NAME)
        .setCustomConfig(new TableCustomConfig(customConfigs))
        .build();
  }

  private static TableConfig maskedEditedConfig(TableConfig stored) {
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getCustomConfig().getCustomConfigs().put(CLIENT_ID_KEY, EDITED_CLIENT_ID);
    return submitted;
  }

  private static String editTableConfigResponse(String response, TableType tableType)
      throws Exception {
    ObjectNode responseNode = (ObjectNode) JsonUtils.stringToJsonNode(response);
    ObjectNode customConfigs = (ObjectNode) responseNode.path(RedactedTableConfigResponse.CONFIGS_KEY)
        .path(tableType.name()).path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs");
    customConfigs.put(CLIENT_ID_KEY, EDITED_CLIENT_ID);
    return responseNode.toString();
  }

  private static String editTableConfigsResponse(String response, TableType tableType)
      throws Exception {
    ObjectNode responseNode = (ObjectNode) JsonUtils.stringToJsonNode(response);
    String configKey = tableType == TableType.OFFLINE ? "offline" : "realtime";
    ObjectNode customConfigs = (ObjectNode) responseNode.path(RedactedTableConfigsResponse.CONFIGS_KEY)
        .path(configKey).path(TableConfig.CUSTOM_CONFIG_KEY).path("customConfigs");
    customConfigs.put(CLIENT_ID_KEY, EDITED_CLIENT_ID);
    return responseNode.toString();
  }

  private static String tableConfigEnvelope(TableConfig tableConfig, int baseVersion)
      throws Exception {
    String tableType = tableConfig.getTableType().name();
    Map<String, Integer> baseVersions = new HashMap<>();
    baseVersions.put(RedactedTableConfigResponse.SCHEMA_VERSION_KEY, SCHEMA_VERSION);
    baseVersions.put(tableType, baseVersion);
    return JsonUtils.objectToString(
        new RedactedTableConfigResponse(Map.of(tableType, tableConfig), baseVersions));
  }

  private static String tableConfigsEnvelope(TableConfigs tableConfigs, Map<String, Integer> baseVersions)
      throws Exception {
    Map<String, Integer> completeBaseVersions = new HashMap<>();
    completeBaseVersions.put(RedactedTableConfigsResponse.SCHEMA_VERSION_KEY, SCHEMA_VERSION);
    completeBaseVersions.put(RedactedTableConfigsResponse.OFFLINE_VERSION_KEY,
        RedactedTableConfigsResponse.ABSENT_VERSION);
    completeBaseVersions.put(RedactedTableConfigsResponse.REALTIME_VERSION_KEY,
        RedactedTableConfigsResponse.ABSENT_VERSION);
    completeBaseVersions.putAll(baseVersions);
    return JsonUtils.objectToString(new RedactedTableConfigsResponse(tableConfigs, completeBaseVersions));
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("id", FieldSpec.DataType.STRING).build();
  }

  private static HttpHeaders headersWithoutDatabase() {
    return mock(HttpHeaders.class);
  }

  private static void assertRestoredCredentialAndEdit(TableConfig updatedConfig) {
    Map<String, String> customConfigs = updatedConfig.getCustomConfig().getCustomConfigs();
    assertEquals(customConfigs.get(LITERAL_PASSWORD_KEY), LITERAL_PASSWORD);
    assertEquals(customConfigs.get(PLACEHOLDER_PASSWORD_KEY), PASSWORD_PLACEHOLDER);
    assertEquals(customConfigs.get(CLIENT_ID_KEY), EDITED_CLIENT_ID);
  }
}
