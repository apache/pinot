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

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Focused response-policy tests for materialized-view details backed by mocked ZK metadata.
public class PinotMaterializedViewRestletResourceTest {
  private static final String RAW_TABLE_NAME = "mv_events";
  private static final String TABLE_NAME_WITH_TYPE = RAW_TABLE_NAME + "_OFFLINE";

  @Test
  public void testDetailsUseUnresolvedConfigAndRedactDefinitionStrings()
      throws Exception {
    String staleDefinitionSql = "SELECT * FROM events WHERE callback = "
        + "'https://stale-user:stale-password@stale.example/path?token=stale-token'";
    String currentSql = "SELECT eventTime, city, count(*) AS cnt FROM events "
        + "WHERE callback = "
        + "'https://${MV_URI_USER}:literal-password@registry.example/path?access_token=${MV_URI_TOKEN}&region=west' "
        + "AND status = 'PAID' GROUP BY eventTime, city";
    Map<String, String> partitionExprMaps = new LinkedHashMap<>();
    partitionExprMaps.put(
        "lookup('https://map-user:map-password@format.example/path?token=map-token&region=west')",
        "https://value-user:value-password@value.example/path?access_token=value-token&region=east");
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        TABLE_NAME_WITH_TYPE, List.of("events_OFFLINE"), staleDefinitionSql, partitionExprMaps, null);
    TableConfig unresolvedConfig = materializedViewConfig(currentSql);
    PinotMaterializedViewRestletResource resource = resourceWith(definition, unresolvedConfig);

    JsonNode response = JsonUtils.stringToJsonNode(resource.getMaterializedView(TABLE_NAME_WITH_TYPE));
    JsonNode responseDefinition = response.path("definition");
    String displayedSql = responseDefinition.path("definedSQL").asText();
    String displayedExpressions = responseDefinition.path("partitionExprMaps").toString();

    assertFalse(displayedSql.contains("stale-user"), displayedSql);
    assertFalse(displayedSql.contains("stale-password"), displayedSql);
    assertFalse(displayedSql.contains("stale-token"), displayedSql);
    assertFalse(displayedSql.contains("literal-password"), displayedSql);
    assertTrue(displayedSql.contains("${MV_URI_USER}"), displayedSql);
    assertTrue(displayedSql.contains("${MV_URI_TOKEN}"), displayedSql);
    assertTrue(displayedSql.contains("registry.example/path"), displayedSql);
    assertTrue(displayedSql.contains("region=west"), displayedSql);
    assertTrue(displayedSql.contains("status = 'PAID'"), displayedSql);
    assertTrue(displayedSql.contains(TableConfigRedactionUtils.REDACTION_MARKER), displayedSql);

    for (String credential : List.of("map-user", "map-password", "map-token",
        "value-user", "value-password", "value-token", "format.example", "value.example")) {
      assertFalse(displayedExpressions.contains(credential), displayedExpressions);
    }
    assertEquals(responseDefinition.path("partitionExprMaps").path("eventTime").asText(), "eventTime");
    assertEquals(responseDefinition.path("baseTables").get(0).asText(), "events");
    verify(resource._pinotHelixResourceManager)
        .getOfflineTableConfig(RAW_TABLE_NAME, false, false);
    verify(resource._pinotHelixResourceManager)
        .getOfflineTableConfig("events", false, false);
    verify(resource._pinotHelixResourceManager, never()).getTableConfig(TABLE_NAME_WITH_TYPE);
  }

  @Test
  public void testDetailsRebuildPartitionExpressionsFromUnresolvedSql()
      throws Exception {
    String resolvedPartitionLiteral = "RESOLVED_BUCKET_UNIT_SENTINEL";
    String unresolvedSql =
        "SELECT dateTrunc('${MV_BUCKET_UNIT}', eventTime) AS eventTime, city FROM events";
    Map<String, String> resolvedPartitionExprMaps = Map.of(
        "dateTrunc('" + resolvedPartitionLiteral + "', eventTime)", "eventTime");
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        TABLE_NAME_WITH_TYPE, List.of("resolved_events"),
        "SELECT dateTrunc('" + resolvedPartitionLiteral + "', eventTime) AS eventTime FROM resolved_events",
        resolvedPartitionExprMaps, null);
    PinotMaterializedViewRestletResource resource = resourceWith(
        definition, materializedViewConfig(unresolvedSql));

    JsonNode response = JsonUtils.stringToJsonNode(resource.getMaterializedView(TABLE_NAME_WITH_TYPE));
    JsonNode responseDefinition = response.path("definition");
    String displayedSql = responseDefinition.path("definedSQL").asText();
    String displayedExpressions = responseDefinition.path("partitionExprMaps").toString();

    assertTrue(displayedSql.contains("${MV_BUCKET_UNIT}"), displayedSql);
    assertTrue(displayedExpressions.contains("${MV_BUCKET_UNIT}"), displayedExpressions);
    assertFalse(displayedSql.contains(resolvedPartitionLiteral), displayedSql);
    assertFalse(displayedExpressions.contains(resolvedPartitionLiteral), displayedExpressions);
    assertEquals(responseDefinition.path("baseTables").get(0).asText(), "events");
  }

  @Test
  public void testPartitionExpressionRedactionCollisionFailsClosed() {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        TABLE_NAME_WITH_TYPE, List.of("events_OFFLINE"), "stale SQL", Map.of(), null);
    String currentSql = "SELECT "
        + "lookup('https://first-user:first-secret@same.example/path?token=first-token') AS eventTime, "
        + "lookup('https://second-user:second-secret@same.example/path?token=second-token') AS otherTime "
        + "FROM events";
    PinotMaterializedViewRestletResource resource = resourceWith(
        definition, materializedViewConfig(currentSql), viewSchemaWithOtherTime());

    ControllerApplicationException error = expectThrows(ControllerApplicationException.class,
        () -> resource.getMaterializedView(TABLE_NAME_WITH_TYPE));

    assertEquals(error.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    for (String credential : List.of("first-user", "first-secret", "first-token",
        "second-user", "second-secret", "second-token")) {
      assertFalse(error.getMessage().contains(credential), error.getMessage());
    }
  }

  @Test
  public void testDetailsEndpointUsesRecognizedTableNameAuthorizationParameter()
      throws Exception {
    Method method = PinotMaterializedViewRestletResource.class.getMethod("getMaterializedView", String.class);

    assertEquals(method.getAnnotation(Path.class).value(), "/materializedViews/{materializedViewTableName}");
    assertEquals(method.getAnnotation(Authorize.class).paramName(), "materializedViewTableName");
  }

  private static TableConfig materializedViewConfig(String definedSql) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME_WITH_TYPE)
        .setIsMaterializedView(true)
        .setTimeColumnName("eventTime")
        .setTaskConfig(new TableTaskConfig(Map.of(
            CommonConstants.MaterializedViewTask.TASK_TYPE,
            Map.of(CommonConstants.MaterializedViewTask.DEFINED_SQL_KEY, definedSql,
                CommonConstants.MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d"))))
        .build();
  }

  private static PinotMaterializedViewRestletResource resourceWith(
      MaterializedViewDefinitionMetadata definition, TableConfig unresolvedConfig) {
    return resourceWith(definition, unresolvedConfig, viewSchema());
  }

  @SuppressWarnings("unchecked")
  private static PinotMaterializedViewRestletResource resourceWith(
      MaterializedViewDefinitionMetadata definition, TableConfig unresolvedConfig, Schema viewSchema) {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String definitionPath =
        ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(TABLE_NAME_WITH_TYPE);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    when(resourceManager.getOfflineTableConfig(RAW_TABLE_NAME, false, false)).thenReturn(unresolvedConfig);
    when(resourceManager.getOfflineTableConfig("events", false, false)).thenReturn(sourceTableConfig());
    when(resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(viewSchema);
    when(resourceManager.getSchema("events")).thenReturn(sourceSchema());
    when(propertyStore.get(eq(definitionPath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(definition.toZNRecord());

    PinotMaterializedViewRestletResource resource = new PinotMaterializedViewRestletResource();
    resource._pinotHelixResourceManager = resourceManager;
    return resource;
  }

  private static TableConfig sourceTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events_OFFLINE")
        .setTimeColumnName("eventTime")
        .build();
  }

  private static Schema viewSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addDateTime("eventTime", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
  }

  private static Schema viewSchemaWithOtherTime() {
    return new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addDateTime("eventTime", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .addDateTime("otherTime", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
  }

  private static Schema sourceSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addDateTime("eventTime", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
  }
}
