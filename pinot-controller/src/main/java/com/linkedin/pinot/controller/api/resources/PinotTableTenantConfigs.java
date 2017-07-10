/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableTenantConfigs extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      PinotTableTenantConfigs.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableTenantConfigs() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Deprecated
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    try {
      return updateTenantConfig("dummy");
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating tenant config " , e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_UPDATE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @Deprecated
  @HttpVerb("put")
  @Summary("DEPRECATED: Updates the tenant configuration for a table")
  @Tags({"table"})
  @Paths({
      "/tables/{tableName}/tenantConfigs"
  })
  private Representation updateTenantConfig(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to update the tenant configuration", required = true)
      String tableName) {
    throw new UnsupportedOperationException("current tenant config updates are not supported");
  }

  @Override
  @Post()
  public Representation post(Representation entity) {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    try {
      return rebuildBrokerResourceFromHelixTags(tableName);
    } catch (Exception e) {
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("post")
  @Summary("Rebuild broker mapping from Helix tags")
  @Tags({"table", "tenant"})
  @Paths({
      "/tables/{tableName}/rebuildBrokerResourceFromHelixTags"
  })
  private Representation rebuildBrokerResourceFromHelixTags(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to rebuild the broker mapping (eg. myTable_OFFLINE or myTable_REALTIME)", required = true)
      String tableName
  ) {
    final PinotResourceManagerResponse pinotResourceManagerResponse =
        _pinotHelixResourceManager.rebuildBrokerResourceFromHelixTags(tableName);

    if (!pinotResourceManagerResponse.isSuccessful()) {
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }

    return new StringRepresentation(pinotResourceManagerResponse.toString());
  }
}
