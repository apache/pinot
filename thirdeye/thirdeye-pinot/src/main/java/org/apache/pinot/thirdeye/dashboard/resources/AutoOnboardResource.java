/*
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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboard;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardUtility;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;


/**
 * Endpoints for triggering adhoc onboard on auto onboard services
 */
@Path(value = "/autoOnboard")
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DASHBOARD_TAG})
public class AutoOnboardResource {

  private Map<String, List<AutoOnboard>> dataSourceToOnboardMap;

  @Inject
  public AutoOnboardResource(ThirdEyeConfiguration thirdeyeConfig) {
    dataSourceToOnboardMap = AutoOnboardUtility.getDataSourceToAutoOnboardMap(thirdeyeConfig.getDataSourcesAsUrl());
  }

  @POST
  @Path("/runAdhoc/{datasource}")
  @ApiOperation("run auto onboard for a data source")
  public Response runAdhocOnboard(@PathParam("datasource") String datasource) {
    if (!dataSourceToOnboardMap.containsKey(datasource)) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Data source %s does not exist in config", datasource)).build();
    }

    for (AutoOnboard autoOnboard : dataSourceToOnboardMap.get(datasource)) {
      autoOnboard.runAdhoc();
    }
    return Response.ok().build();
  }

}
