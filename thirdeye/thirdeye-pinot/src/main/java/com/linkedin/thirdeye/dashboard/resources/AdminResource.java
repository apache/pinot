/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.api.Constants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.linkedin.thirdeye.dashboard.views.ThirdEyeAdminView;

import io.dropwizard.views.View;

@Path(value = "/thirdeye-admin")
@Api(tags = {Constants.ADMIN_TAG } )
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

  @GET
  @Path(value = "/")
  @ApiOperation(value = "Load the ThirdEye admin dashboard.")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new ThirdEyeAdminView();
  }
}
