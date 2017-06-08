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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableSchema extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSchema.class);

  public PinotTableSchema()
      throws IOException {
    File baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    File schemaTempDir = new File(baseDataDir, "schemasTemp");
    if (!schemaTempDir.exists()) {
      FileUtils.forceMkdir(schemaTempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    String tableName = (String) getRequest().getAttributes().get("tableName");
    return getTableSchema(tableName);
  }

  @HttpVerb("get")
  @Summary("Gets schema for a table")
  @Tags({"schema", "table"})
  @Paths({"/tables/{tableName}/schema", "/tables/{tableName}/schema/"})
  private Representation getTableSchema(
      @Parameter(name = "tableName", in = "path", description = "Table name for which to get the schema",
          required = true) String tableName) {
    Schema schema = _pinotHelixResourceManager.getTableSchema(tableName);
    if (schema != null) {
      return new StringRepresentation(schema.getJSONSchema());
    } else {
      LOGGER.info("Failed to fetch schema for table: {}", tableName);
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Error: schema not found for table: " + tableName);
    }
  }
}
