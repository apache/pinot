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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadataUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/// REST endpoints for the Materialized View UI: list, describe, drop.
///
/// Read endpoints hydrate from the ZK definition + runtime znodes that the consistency manager
/// and minion task executor maintain. The drop endpoint delegates to
/// `PinotHelixResourceManager.deleteTable` so the same dependent-MV guards and segment-cleanup
/// paths apply.
///
/// Authorization model: the per-name endpoints (GET, DELETE) are scoped to `TargetType.TABLE`
/// with the path param as the resource identifier, so an operator that owns the underlying MV
/// table can manage it via these endpoints just as they would via `/tables/{name}`. The listing
/// endpoint stays cluster-scoped (mirrors `GET /tables`).
@Api(tags = Constants.MATERIALIZED_VIEW_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")}))
@Path("/")
public class PinotMaterializedViewRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotMaterializedViewRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/materializedViews")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE)
  @ApiOperation(value = "List materialized views",
      notes = "Returns a summary entry for every MV definition znode in the cluster: name, base tables, "
          + "watermarkMs, number of VALID/STALE partitions, and last refresh time.")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success")})
  public String listMaterializedViews() {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    String definitionParent = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    List<String> viewTableNames = propertyStore.getChildNames(definitionParent, AccessOption.PERSISTENT);

    ArrayNode mvs = JsonUtils.newArrayNode();
    if (viewTableNames != null) {
      for (String viewTableName : viewTableNames) {
        try {
          ObjectNode summary = buildSummary(viewTableName);
          if (summary != null) {
            mvs.add(summary);
          }
        } catch (Exception e) {
          // One broken znode must not break the entire listing — surface it as a placeholder
          // entry that carries `error` so the UI can render it as a problem row.
          LOGGER.warn("Failed to summarize MV {}: {}", viewTableName, e.getMessage());
          ObjectNode err = JsonUtils.newObjectNode();
          err.put("materializedViewTableName", viewTableName);
          err.put("error", e.getMessage());
          mvs.add(err);
        }
      }
    }
    ObjectNode result = JsonUtils.newObjectNode();
    result.set("materializedViews", mvs);
    return result.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/materializedViews/{materializedViewTableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "materializedViewTableName",
      action = Actions.Table.GET_TABLE_CONFIG)
  @ApiOperation(value = "Get materialized view definition + runtime",
      notes = "Returns the full ZK definition (definedSQL, base tables, split spec, partition expressions, "
          + "stalenessThresholdMs) and runtime metadata (watermarkMs, per-bucket state map).")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "MV not found")})
  public String getMaterializedView(
      @ApiParam(value = "MV table name (with type suffix, e.g. airlineStatsMv_OFFLINE)", required = true)
      @PathParam("materializedViewTableName") String viewTableName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    MaterializedViewDefinitionMetadata definition =
        MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, viewTableName);
    if (definition == null) {
      throw new ControllerApplicationException(LOGGER,
          "Materialized view not found: " + viewTableName, Response.Status.NOT_FOUND);
    }
    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadataUtils.fetch(propertyStore, viewTableName);

    ObjectNode result = JsonUtils.newObjectNode();
    // Pass the already-fetched runtime so the partition-summary aggregates and the per-bucket
    // table are computed from the SAME ZK snapshot.  Two reads would risk rendering
    // VALID/STALE counts that disagree with the partition states on the same page.
    result.set("definition", buildDefinitionNode(definition, runtime));
    result.set("runtime", buildRuntimeNode(runtime));
    return result.toString();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/materializedViews/{materializedViewTableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "materializedViewTableName",
      action = Actions.Table.DELETE_TABLE)
  @ApiOperation(value = "Drop a materialized view",
      notes = "Drops both the underlying Pinot table (segments, table config, schema) and the MV definition + "
          + "runtime znodes. Equivalent to DELETE /tables/{name} followed by ZK cleanup, but routes through the "
          + "table-delete path so the same dependent-MV safety checks apply (an MV-over-MV is rejected).")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "MV not found"),
      @ApiResponse(code = 409, message = "MV cannot be dropped because other MVs depend on it")})
  public String deleteMaterializedView(
      @ApiParam(value = "MV table name (with type suffix)", required = true)
      @PathParam("materializedViewTableName") String viewTableName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    MaterializedViewDefinitionMetadata definition =
        MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, viewTableName);
    if (definition == null) {
      throw new ControllerApplicationException(LOGGER,
          "Materialized view not found: " + viewTableName, Response.Status.NOT_FOUND);
    }
    // Definition znode is keyed by the fully-qualified name (`<rawName>_OFFLINE`), so a
    // successful fetch above guarantees `viewTableName` carries a recognizable type suffix.
    // Any subsequent code-path bug that lets a raw name through must fail loud, not silently
    // default to OFFLINE.
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(viewTableName);
    Preconditions.checkState(tableType != null,
        "MV definition fetch succeeded for unrecognized table name format: %s", viewTableName);
    try {
      // Delegate to the regular table-delete path. PinotHelixResourceManager.deleteTable already
      // unregisters from the consistency manager and removes the MV definition + runtime znodes
      // via MaterializedViewDefinitionMetadataUtils.delete + MaterializedViewRuntimeMetadataUtils.delete.
      _pinotHelixResourceManager.deleteTable(viewTableName, tableType, null);
    } catch (IllegalStateException e) {
      // Thrown when a dependent MV blocks the delete.
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to drop materialized view " + viewTableName + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    ObjectNode result = JsonUtils.newObjectNode();
    result.put("status", "Materialized view " + viewTableName + " dropped.");
    return result.toString();
  }

  /// Builds the listing-row summary (small, suitable for the listing page table).
  private ObjectNode buildSummary(String viewTableName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    MaterializedViewDefinitionMetadata definition =
        MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, viewTableName);
    if (definition == null) {
      return null;
    }
    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadataUtils.fetch(propertyStore, viewTableName);

    ObjectNode summary = JsonUtils.newObjectNode();
    summary.put("materializedViewTableName", definition.getMaterializedViewTableNameWithType());
    summary.set("baseTables", JsonUtils.objectToJsonNode(definition.getBaseTables()));
    summary.put("stalenessThresholdMs", definition.getStalenessThresholdMs());

    if (runtime == null) {
      summary.put("watermarkMs", 0L);
      summary.put("validPartitions", 0);
      summary.put("stalePartitions", 0);
      summary.put("totalPartitions", 0);
      summary.putNull("lastRefreshTime");
      return summary;
    }
    summary.put("watermarkMs", runtime.getWatermarkMs());
    int valid = 0;
    int stale = 0;
    long latestRefresh = 0L;
    for (PartitionInfo info : runtime.getPartitions().values()) {
      if (info.getState() == PartitionState.VALID) {
        valid++;
      } else if (info.getState() == PartitionState.STALE) {
        stale++;
      }
      if (info.getLastRefreshTime() > latestRefresh) {
        latestRefresh = info.getLastRefreshTime();
      }
    }
    summary.put("validPartitions", valid);
    summary.put("stalePartitions", stale);
    summary.put("totalPartitions", runtime.getPartitions().size());
    if (latestRefresh > 0L) {
      summary.put("lastRefreshTime", latestRefresh);
    } else {
      summary.putNull("lastRefreshTime");
    }
    return summary;
  }

  private ObjectNode buildDefinitionNode(MaterializedViewDefinitionMetadata definition,
      MaterializedViewRuntimeMetadata runtime) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("materializedViewTableName", definition.getMaterializedViewTableNameWithType());
    node.put("definedSQL", definition.getDefinedSql());
    node.set("baseTables", JsonUtils.objectToJsonNode(definition.getBaseTables()));
    node.set("partitionExprMaps", JsonUtils.objectToJsonNode(definition.getPartitionExprMaps()));
    node.put("stalenessThresholdMs", definition.getStalenessThresholdMs());
    // Summary partition stats computed from the SAME runtime snapshot the partition table
    // below uses, so the two views can never disagree on a single page render.
    appendPartitionSummary(node, runtime);
    if (definition.getSplitSpec() != null) {
      ObjectNode split = JsonUtils.newObjectNode();
      split.put("sourceTimeColumn", definition.getSplitSpec().getSourceTimeColumn());
      split.put("sourceTimeFormat", definition.getSplitSpec().getSourceTimeFormat());
      split.put("materializedViewTimeColumn", definition.getSplitSpec().getMaterializedViewTimeColumn());
      split.put("bucketMs", definition.getSplitSpec().getBucketMs());
      node.set("splitSpec", split);
    } else {
      node.putNull("splitSpec");
    }
    return node;
  }

  /// Counts VALID / STALE / total partitions from the supplied runtime snapshot and attaches
  /// them to the definition response. The details UI shows these in the Summary block; without
  /// this the page would have to scan the partition array three times client-side.
  private static void appendPartitionSummary(ObjectNode definitionNode, MaterializedViewRuntimeMetadata runtime) {
    int valid = 0;
    int stale = 0;
    int total = 0;
    if (runtime != null) {
      total = runtime.getPartitions().size();
      for (PartitionInfo info : runtime.getPartitions().values()) {
        if (info.getState() == PartitionState.VALID) {
          valid++;
        } else if (info.getState() == PartitionState.STALE) {
          stale++;
        }
      }
    }
    definitionNode.put("validPartitions", valid);
    definitionNode.put("stalePartitions", stale);
    definitionNode.put("totalPartitions", total);
  }

  private ObjectNode buildRuntimeNode(MaterializedViewRuntimeMetadata runtime) {
    if (runtime == null) {
      return JsonUtils.newObjectNode().put("absent", true);
    }
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("watermarkMs", runtime.getWatermarkMs());
    ArrayNode partitions = JsonUtils.newArrayNode();
    Map<Long, PartitionInfo> partitionMap = runtime.getPartitions();
    // Emit partitions in ascending bucket-start order so the UI doesn't need to sort.
    partitionMap.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> {
          ObjectNode p = JsonUtils.newObjectNode();
          p.put("bucketStartMs", entry.getKey());
          p.put("state", entry.getValue().getState().name());
          p.put("segmentCount", entry.getValue().getFingerprint().getSegmentCount());
          p.put("crc", entry.getValue().getFingerprint().getCrcChecksum());
          p.put("lastRefreshTime", entry.getValue().getLastRefreshTime());
          partitions.add(p);
        });
    node.set("partitions", partitions);
    return node;
  }
}
