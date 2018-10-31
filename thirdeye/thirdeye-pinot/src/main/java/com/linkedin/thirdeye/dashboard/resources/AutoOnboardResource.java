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

import com.linkedin.thirdeye.auto.onboard.AutoOnboard;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardUtility;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.DataSources;
import com.linkedin.thirdeye.datasource.DataSourcesLoader;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Endpoints for triggering adhoc onboard on auto onboard services
 */
@Path(value = "/autoOnboard")
@Produces(MediaType.APPLICATION_JSON)
public class AutoOnboardResource {

  private Map<String, List<AutoOnboard>> dataSourceToOnboardMap;

  public AutoOnboardResource(ThirdEyeConfiguration thirdeyeConfig) {
    dataSourceToOnboardMap = AutoOnboardUtility.getDataSourceToAutoOnboardMap(thirdeyeConfig.getDataSourcesAsUrl());
  }

  @POST
  @Path("/runAdhoc/{datasource}")
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
