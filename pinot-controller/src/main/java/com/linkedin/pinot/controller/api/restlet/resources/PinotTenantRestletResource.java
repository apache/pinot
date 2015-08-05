/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.restlet.resources;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.api.swagger.Description;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.ResponseStatus;


/**
 *  Sample curl call to create broker tenant
 *  curl -i -X POST -H 'Content-Type: application/json' -d
 *  '{
 *    "role" : "broker",
 *    "numberOfInstances : "5",
 *    "name" : "brokerOne"
 *    }' http://lva1-pinot-controller-vip-1.corp.linkedin.com:11984/tenants
 *
 *  Sample curl call to create server tenant
 *  curl -i -X POST -H 'Content-Type: application/json' -d
 *  '{
 *    "role" : "server",
 *    "numberOfInstances : "5",
 *    "name" : "serverOne",
 *    "offlineInstances" : "3",
 *    "realtimeInstances" : "2"
 *    }' http://lva1-pinot-controller-vip-1.corp.linkedin.com:11984/tenants
 */

public class PinotTenantRestletResource extends PinotRestletResourceBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTenantRestletResource.class);

  private static final String TENANT_NAME = "tenantName";

  private final ObjectMapper _objectMapper;

  public PinotTenantRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    _objectMapper = new ObjectMapper();
  }

  /*
   * For tenant creation
   */
  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation;
    try {
      final Tenant tenant = _objectMapper.readValue(entity.getText(), Tenant.class);
      presentation = createTenant(tenant);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing put request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("post")
  @Summary("Creates a tenant")
  @Tags({ "tenant" })
  @Paths({ "/tenants", "/tenants/" })
  private StringRepresentation createTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    StringRepresentation presentation;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = _pinotHelixResourceManager.createBrokerTenant(tenant);
        presentation = new StringRepresentation(response.toString());
        break;
      case SERVER:
        response = _pinotHelixResourceManager.createServerTenant(tenant);
        presentation = new StringRepresentation(response.toString());
        break;
      default:
        throw new RuntimeException("Not a valid tenant creation call");
    }
    return presentation;
  }

  /*
   * For tenant update
   */
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    StringRepresentation presentation;
    try {
      final Tenant tenant = _objectMapper.readValue(ByteStreams.toByteArray(entity.getStream()), Tenant.class);
      presentation = updateTenant(tenant);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing put request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("put")
  @Summary("Updates a tenant")
  @Tags({ "tenant" })
  @Paths({ "/tenants", "/tenants/" })
  private StringRepresentation updateTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    StringRepresentation presentation;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = _pinotHelixResourceManager.updateBrokerTenant(tenant);
        presentation = new StringRepresentation(response.toString());
        break;
      case SERVER:
        response = _pinotHelixResourceManager.updateServerTenant(tenant);
        presentation = new StringRepresentation(response.toString());
        break;
      default:
        throw new RuntimeException("Not a valid tenant update call");
    }
    return presentation;
  }

  /**
   * URI Mappings:
   * "/tenants", "/tenants/": List all the tenants in the cluster.
   * "/tenants/{tenantName}", "/tenants/{tenantName}/": List all instances for the tenant.
   * "/tenants/{tenantName}?state={state}":
   * - Set the state for the specified tenant to the specified value, one of {enable|disable|drop}.
   *
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#get()
   */
  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String tenantName = (String) getRequest().getAttributes().get(TENANT_NAME);
      final String state = getReference().getQueryAsForm().getValues(STATE);
      ;
      final String type = getReference().getQueryAsForm().getValues(TABLE_TYPE);

      if (tenantName == null) {
        presentation = getAllTenants(type);
      } else if (state == null) {
        presentation = getTenant(tenantName, type);
      } else {
        if (isValidState(state)) {
          presentation = toggleTenantState(tenantName, state, type);
        } else {
          return new StringRepresentation(INVALID_STATE_ERROR);
        }
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Gets information about a tenant")
  @Tags({ "tenant" })
  @Paths({ "/tenants/{tenantName}", "/tenants/{tenantName}/" })
  private StringRepresentation getTenant(
      @Parameter(name = "tenantName", in = "path", description = "The tenant name") String tenantName, @Parameter(
          name = "type", in = "query", description = "The type of tenant, either SERVER or BROKER") String type)
      throws JSONException {
    StringRepresentation presentation;// Return instances related to given tenant name.

    JSONObject resourceGetRet = new JSONObject();
    if (type == null) {
      resourceGetRet.put("ServerInstances", _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName));
      resourceGetRet.put("BrokerInstances", _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName));
    } else {
      if (type.equalsIgnoreCase("server")) {
        resourceGetRet.put("ServerInstances", _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName));
      }
      if (type.equalsIgnoreCase("broker")) {
        resourceGetRet.put("BrokerInstances", _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName));
      }
    }
    resourceGetRet.put(TENANT_NAME, tenantName);

    presentation = new StringRepresentation(resourceGetRet.toString(), MediaType.APPLICATION_JSON);
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Gets information about a tenant")
  @Tags({ "tenant" })
  @Paths({ "/tenants/{tenantName}", "/tenants/{tenantName}/" })
  private StringRepresentation toggleTenantState(
      @Parameter(name = "tenantName", in = "path", description = "The tenant name")
      String tenantName,
      @Parameter(name = "state", in = "query", description = "state to set for the tenant {enable|disable|drop}")
      String state,
      @Parameter(name = "type", in = "query", description = "The type of tenant, either SERVER or BROKER or NULL")
      String type) throws JSONException {

    Set<String> serverInstances = new HashSet<String>();
    Set<String> brokerInstances = new HashSet<String>();
    JSONObject instanceResult = new JSONObject();

    if ((type == null) || type.equalsIgnoreCase("server")) {
      serverInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
    }

    if ((type == null) || type.equalsIgnoreCase("broker")) {
      brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
    }

    Set<String> allInstances = new HashSet<String>(serverInstances);
    allInstances.addAll(brokerInstances);

    if (StateType.DROP.name().equalsIgnoreCase(state)) {
      if (!allInstances.isEmpty()) {
        return new StringRepresentation("Error: Tenant " + tenantName + " has live instances, cannot be dropped.");
      }
      _pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
      return new StringRepresentation("Dropped tenant " + tenantName + " successfully.");
    }

    boolean enable = StateType.ENABLE.name().equalsIgnoreCase(state) ? true : false;
    for (String instance : allInstances) {
      if (enable) {
        instanceResult.put(instance, _pinotHelixResourceManager.enableInstance(instance));
      } else {
        instanceResult.put(instance, _pinotHelixResourceManager.disableInstance(instance));
      }
    }

    return new StringRepresentation(instanceResult.toString(), MediaType.APPLICATION_JSON);
  }

  @HttpVerb("get")
  @Summary("Gets information about all tenants")
  @Tags({ "tenant" })
  @Paths({ "/tenants", "/tenants/" })
  private StringRepresentation getAllTenants(@Parameter(name = "type", in = "query",
      description = "The type of tenant, either SERVER or BROKER") String type) throws JSONException {
    StringRepresentation presentation;// Return all the tags.
    final JSONObject ret = new JSONObject();
    if (type == null || type.equalsIgnoreCase("server")) {
      ret.put("SERVER_TENANTS", _pinotHelixResourceManager.getAllServerTenantNames());
    }
    if (type == null || type.equalsIgnoreCase("broker")) {
      ret.put("BROKER_TENANTS", _pinotHelixResourceManager.getAllBrokerTenantNames());
    }
    presentation = new StringRepresentation(ret.toString(), MediaType.APPLICATION_JSON);
    return presentation;
  }

  @Override
  @Delete
  public Representation delete() {
    StringRepresentation presentation;
    try {
      final String tenantName = (String) getRequest().getAttributes().get(TENANT_NAME);
      final String type = getReference().getQueryAsForm().getValues("type");
      presentation = deleteTenant(tenantName, type);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing delete request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("delete")
  @Summary("Deletes a tenant")
  @Tags({ "tenant" })
  @Description("Deletes a tenant from the cluster")
  @Paths({ "/tenants/{tenantName}", "/tenants/{tenantName}/" })
  private StringRepresentation deleteTenant(
      @Parameter(name = "tenantName", in = "path", description = "The tenant id") String tenantName,
      @Parameter(name = "type", in = "query", description = "The type of tenant, either SERVER or BROKER",
          required = true) String type) {
    StringRepresentation presentation;
    if (type == null) {
      presentation =
          new StringRepresentation("Not specify the type for the tenant name. Please try to append:"
              + "/?type=SERVER or /?type=BROKER ");
    } else {
      TenantRole tenantRole = TenantRole.valueOf(type.toUpperCase());
      PinotResourceManagerResponse res = null;
      switch (tenantRole) {
        case BROKER:
          if (_pinotHelixResourceManager.isBrokerTenantDeletable(tenantName)) {
            res = _pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
          } else {
            res = new PinotResourceManagerResponse();
            res.status = ResponseStatus.failure;
            res.message = "Broker Tenant is not null, cannot delete it.";
          }
          break;
        case SERVER:
          if (_pinotHelixResourceManager.isServerTenantDeletable(tenantName)) {
            res = _pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
            if (res.isSuccessfull()) {
              res = _pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
            }
          } else {
            res = new PinotResourceManagerResponse();
            res.status = ResponseStatus.failure;
            res.message = "Server Tenant is not null, cannot delete it.";
          }
          break;
        default:
          break;
      }
      presentation = new StringRepresentation(res.toString());
    }
    return presentation;
  }

  private StringRepresentation exceptionToStringRepresentation(Exception e) {
    return new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
  }
}
