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

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;


/**
 * API endpoint that returns the versions of Pinot components.
 */
public class PinotVersionRestletResource extends BasePinotControllerRestletResource {
  @Override
  @Get
  public Representation get() {
    return buildVersionResponse();
  }

  @HttpVerb("get")
  @Summary("Obtains the version number of the Pinot components")
  @Tags({ "version" })
  @Paths({ "/version" })
  private Representation buildVersionResponse() {
    JSONObject jsonObject = new JSONObject(Utils.getComponentVersions());
    return new StringRepresentation(jsonObject.toString(), MediaType.APPLICATION_JSON);
  }
}
