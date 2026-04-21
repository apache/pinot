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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.version.ClusterVersionSummary;
import org.apache.pinot.controller.helix.core.version.CompatibilityCheckResult;
import org.apache.pinot.controller.helix.core.version.ComponentVersionSummary;
import org.apache.pinot.controller.helix.core.version.VersionCompatibilityService;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * REST endpoints that expose cluster-wide version information and rollout-order compatibility
 * checks.
 *
 * <pre>
 *   GET /cluster/versions              — full ClusterVersionSummary (all component types)
 *   GET /cluster/versions?componentType=SERVER  — filtered to one component type
 *   GET /cluster/compatibility         — rollout-order check result + full version summary
 * </pre>
 *
 * <p>All responses are advisory. No operation is blocked based on these results.
 */
@Api(tags = Constants.VERSION_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(
    name = HttpHeaders.AUTHORIZATION,
    in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
    key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("/")
public class ClusterVersionResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionResource.class);
  private static final Set<String> VALID_COMPONENT_TYPES = Set.of("CONTROLLER", "BROKER", "SERVER", "MINION");

  @Inject
  VersionCompatibilityService _versionCompatibilityService;

  /**
   * Returns the cluster-wide version summary, optionally filtered to a single component type.
   *
   * <p>The response reflects a cached snapshot of Helix InstanceConfig data. The
   * {@code snapshotTimeMs} field indicates when the snapshot was last refreshed.
   *
   * @param componentType optional filter: {@code CONTROLLER}, {@code BROKER}, {@code SERVER},
   *                      or {@code MINION}. When absent all component types are returned.
   */
  @GET
  @Path("/cluster/versions")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get cluster-wide version summary for all or one component type")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Unknown componentType value"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getClusterVersions(
      @ApiParam(value = "Filter by component type: CONTROLLER, BROKER, SERVER, MINION")
      @QueryParam("componentType") String componentType) {
    try {
      ClusterVersionSummary summary = _versionCompatibilityService.getClusterVersionSummary();

      if (componentType != null) {
        String type = componentType.toUpperCase().trim();
        if (!VALID_COMPONENT_TYPES.contains(type)) {
          throw new ControllerApplicationException(LOGGER,
              "Unknown componentType '" + componentType + "'. Valid values: CONTROLLER, BROKER, SERVER, MINION",
              Response.Status.BAD_REQUEST);
        }
        // When data is unavailable, component summaries are empty. Return the summary as-is so
        // callers can distinguish "unavailable" (dataAvailable=false) from "bad input".
        ComponentVersionSummary comp = summary.getComponentSummaries().get(type);
        Map<String, ComponentVersionSummary> filteredMap =
            (comp == null) ? Collections.emptyMap() : Collections.singletonMap(type, comp);
        ClusterVersionSummary filtered = new ClusterVersionSummary(
            summary.getClusterName(), summary.getSnapshotTimeMs(), summary.isDataAvailable(), filteredMap);
        return JsonUtils.objectToString(filtered);
      }

      return JsonUtils.objectToString(summary);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to get cluster versions: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Evaluates the rollout-order compatibility check and returns the result together with the
   * full cluster version snapshot.
   *
   * <p>A non-{@code ok} result means warnings were detected (e.g. a broker is running a
   * newer version than the controller). All findings are advisory — no operation is blocked.
   * Offline-instance counts are available via the embedded
   * {@link ClusterVersionSummary#getComponentSummaries()} field.
   */
  @GET
  @Path("/cluster/compatibility")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check cluster version compatibility (rollout-order validation)")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getCompatibility() {
    try {
      CompatibilityCheckResult result = _versionCompatibilityService.checkRolloutOrderCompatibility();
      return JsonUtils.objectToString(result);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to check cluster compatibility: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
