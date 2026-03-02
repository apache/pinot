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
package org.apache.pinot.minion.api.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.spi.utils.JsonUtils;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Metrics", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/metrics")
public class PinotMinionMetricsResource {
  public static final String METRIC_EXISTS = "metricExists";
  public static final String GAUGE_VALUE = "gaugeValue";
  private final MinionMetrics _minionMetrics = MinionMetrics.get();

  @GET
  @Path("/gauge/{gaugeName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get gauge value for the provided minion gauge name")
  public String getMinionGaugeValue(@ApiParam(value = "Gauge name") @PathParam("gaugeName") String gaugeName)
      throws JsonProcessingException {
    Long value = _minionMetrics.getGaugeValue(gaugeName);
    Map<String, Object> response = new HashMap<>();
    if (value != null) {
      response.put(GAUGE_VALUE, value);
      response.put(METRIC_EXISTS, true);
    } else {
      response.put(METRIC_EXISTS, false);
    }
    return JsonUtils.objectToString(response);
  }
}
