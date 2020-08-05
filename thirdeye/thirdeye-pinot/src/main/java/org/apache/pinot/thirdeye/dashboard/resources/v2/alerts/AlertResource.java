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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2.alerts;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.api.Constants;


/**
 * The Alert resource.
 */
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DETECTION_TAG})
public class AlertResource {
  private final AlertSearcher alertSearcher;

  /**
   * Instantiates a new Alert resource.
   */
  public AlertResource() {
    this.alertSearcher = new AlertSearcher();
  }

  /**
   * Search the alerts with result pagination. It will return record from No.(offset+1) to record No.(offset + limit).
   *
   * The endpoint will return paginated results for alerts that pass all the filters. Within each filter option,
   * the result only need to pass either one of them.
   *
   * @param limit the returned result limit
   * @param offset the offset of the start position
   * @param applications the applications for the alerts
   * @param subscriptionGroups the subscription groups for the alerts
   * @param names the names for the alerts
   * @param createdBy the owners for the alerts
   * @param subscribedBy the subscriber for the alerts
   * @param ruleTypes the rule types for the alerts
   * @param metrics the metrics for the alerts
   * @param datasets the datasets for the alerts
   * @param active if the alert is active
   * @return the response
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Search and paginate alerts according to the parameters")
  public Response findAlerts(@QueryParam("limit") @DefaultValue("10") long limit,
      @QueryParam("offset") @DefaultValue("0") long offset, @QueryParam("application") List<String> applications,
      @QueryParam("subscriptionGroup") List<String> subscriptionGroups, @QueryParam("names") List<String> names,
      @QueryParam("createdBy") List<String> createdBy, @QueryParam("subscribedBy") List<String> subscribedBy, @QueryParam("ruleType") List<String> ruleTypes,
      @QueryParam("metric") List<String> metrics, @QueryParam("dataset") List<String> datasets,
      @QueryParam("active") Boolean active) {
    AlertSearchFilter searchFilter =
        new AlertSearchFilter(applications, subscriptionGroups, names, createdBy, subscribedBy, ruleTypes, metrics, datasets, active);
    return Response.ok().entity(this.alertSearcher.search(searchFilter, limit, offset)).build();
  }
}
