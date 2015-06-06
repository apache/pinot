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

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;


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

public class PinotTenantRestletResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTenantRestletResource.class);

  private static final String TENANT_NAME = "tenantName";

  private final PinotHelixResourceManager _pinotHelixResourceMananger;
  private final ObjectMapper _objectMapper;

  public PinotTenantRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    _pinotHelixResourceMananger =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    _objectMapper = new ObjectMapper();
  }

  /*
   * For tenant creation
   */
  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation = null;
    try {
      PinotResourceManagerResponse response = null;
      final Tenant tenant = _objectMapper.readValue(entity.getText(), Tenant.class);
      switch (tenant.getTenantRole()) {
        case BROKER:
          response = _pinotHelixResourceMananger.createBrokerTenant(tenant);
          presentation = new StringRepresentation(response.toString());
          break;
        case SERVER:
          response = _pinotHelixResourceMananger.createServerTenant(tenant);
          presentation = new StringRepresentation(response.toString());
          break;
        default:
          throw new RuntimeException("Not a valid tenant creation call");
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing put request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  /*
   * For tenant update
   */
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    StringRepresentation presentation = null;
    try {
      PinotResourceManagerResponse response = null;
      final Tenant tenant = _objectMapper.readValue(ByteStreams.toByteArray(entity.getStream()), Tenant.class);
      switch (tenant.getTenantRole()) {
        case BROKER:
          response = _pinotHelixResourceMananger.updateBrokerTenant(tenant);
          presentation = new StringRepresentation(response.toString());
          break;
        case SERVER:
          response = _pinotHelixResourceMananger.updateServerTenant(tenant);
          presentation = new StringRepresentation(response.toString());
          break;
        default:
          throw new RuntimeException("Not a valid tenant update call");
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing put request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  /**
   *  called with optional resourceName
   *  if resourceName is not present then it sends back a list of
   * @return
   */
  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String tenantName = (String) getRequest().getAttributes().get(TENANT_NAME);
      if (tenantName == null) {
        // Return all the tags.
        final JSONObject ret = new JSONObject();
        final String type = getReference().getQueryAsForm().getValues("type");
        if (type == null || type.equals("server")) {
          ret.put("SERVER_TENANTS", _pinotHelixResourceMananger.getAllServerTenantNames());
        }
        if (type == null || type.equals("broker")) {
          ret.put("BROKER_TENANTS", _pinotHelixResourceMananger.getAllBrokerTenantNames());
        }
        presentation = new StringRepresentation(ret.toString(), MediaType.APPLICATION_JSON);
      } else {
        // Return instances related to given tenant name.
        final String type = getReference().getQueryAsForm().getValues("type");

        JSONObject resourceGetRet = new JSONObject();
        if (type == null) {
          resourceGetRet.put("ServerInstances", _pinotHelixResourceMananger.getAllInstancesForServerTenant(tenantName));
          resourceGetRet.put("BrokerInstances", _pinotHelixResourceMananger.getAllInstancesForBrokerTenant(tenantName));
        } else {
          if (type.equals("server")) {
            resourceGetRet.put("ServerInstances",
                _pinotHelixResourceMananger.getAllInstancesForServerTenant(tenantName));
          }
          if (type.equals("broker")) {
            resourceGetRet.put("BrokerInstances",
                _pinotHelixResourceMananger.getAllInstancesForBrokerTenant(tenantName));
          }
        }
        resourceGetRet.put(TENANT_NAME, tenantName);

        presentation = new StringRepresentation(resourceGetRet.toString(), MediaType.APPLICATION_JSON);
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @Override
  @Delete
  public Representation delete() {
    StringRepresentation presentation = null;
    try {
      final String tenantName = (String) getRequest().getAttributes().get(TENANT_NAME);
      final String type = getReference().getQueryAsForm().getValues("type");
      if (type == null) {
        presentation =
            new StringRepresentation("Not specify the type for the tenant name. Please try to append:"
                + "/?type=SERVER or /?type=BROKER ");
      } else {
        TenantRole tenantRole = TenantRole.valueOf(type.toUpperCase());
        PinotResourceManagerResponse res = null;
        switch (tenantRole) {
          case BROKER:
            if (_pinotHelixResourceMananger.isBrokerTenantDeletable(tenantName)) {
              res = _pinotHelixResourceMananger.deleteBrokerTenantFor(tenantName);
            } else {
              res = new PinotResourceManagerResponse();
              res.status = STATUS.failure;
              res.errorMessage = "Broker Tenant is not null, cannot delete it.";
            }
            break;
          case SERVER:
            if (_pinotHelixResourceMananger.isServerTenantDeletable(tenantName)) {
              res = _pinotHelixResourceMananger.deleteOfflineServerTenantFor(tenantName);
              if (res.isSuccessfull()) {
                res = _pinotHelixResourceMananger.deleteRealtimeServerTenantFor(tenantName);
              }
            } else {
              res = new PinotResourceManagerResponse();
              res.status = STATUS.failure;
              res.errorMessage = "Server Tenant is not null, cannot delete it.";
            }
            break;
          default:
            break;
        }
        presentation = new StringRepresentation(res.toString());
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing delete request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  private StringRepresentation exceptionToStringRepresentation(Exception e) {
    return new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
  }

}
