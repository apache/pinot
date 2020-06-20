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

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import com.google.gson.Gson;
import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.common.restclient.ThirdEyeRcaRestClient;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.RCAFramework;
import org.apache.pinot.thirdeye.rootcause.RCAFrameworkExecutionResult;
import org.apache.pinot.thirdeye.rootcause.impl.TimeRangeEntity;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/rootcause")
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.RCA_TAG})
public class RootCauseResource {
  private static final Logger LOG = LoggerFactory.getLogger(RootCauseResource.class);

  private static final int DEFAULT_FORMATTER_DEPTH = 1;

  private static final long ANALYSIS_RANGE_MAX = TimeUnit.DAYS.toMillis(32);
  private static final long ANOMALY_RANGE_MAX = TimeUnit.DAYS.toMillis(32);
  private static final long BASELINE_RANGE_MAX = ANOMALY_RANGE_MAX;

  private final List<RootCauseEntityFormatter> formatters;
  private final Map<String, RCAFramework> frameworks;
  private final String dashboardHost;

  public RootCauseResource(String dashboardHost, Map<String, RCAFramework> frameworks, List<RootCauseEntityFormatter> formatters) {
    this.dashboardHost = dashboardHost;
    this.frameworks = frameworks;
    this.formatters = formatters;
  }

  @GET
  @Path("/highlights")
  @ApiOperation(value = "Send query")
  public Map<String, String> highlights(
      @ApiParam(value = "internal id of the anomaly")
      @QueryParam("anomalyId") long anomalyId,
      @Auth ThirdEyePrincipal principal) {
    Map<String, String> responseMessage = new HashMap<>();
    ThirdEyeRcaRestClient rcaClient = new ThirdEyeRcaRestClient(principal, dashboardHost);

    Map<String, Object> eventHighlights = new HashMap<>();
    try {
      eventHighlights = rcaClient.getRelatedEventHighlights(anomalyId);
    } catch (IOException e) {
      LOG.error("Failed to retrieve the RCA Event Highlights", e);
    }
    responseMessage.put("relatedEvents", new Gson().toJson(eventHighlights));

    Map<String, Object> relatedMetricHighlights = new HashMap<>();
    try {
      relatedMetricHighlights = rcaClient.getRelatedMetricHighlights(anomalyId);
    } catch (IOException e) {
      LOG.error("Failed to retrieve the RCA Metric Highlights", e);
    }
    responseMessage.put("relatedMetrics", new Gson().toJson(relatedMetricHighlights));

    Map<String, Object> cubeHighlights = new HashMap<>();
    try {
      cubeHighlights = rcaClient.getCubeHighlights(anomalyId);
    } catch (IOException e) {
      LOG.error("Failed to retrieve the RCA Cube Highlights", e);
    }
    responseMessage.put("cubeResults", new Gson().toJson(cubeHighlights));

    return responseMessage;
  }

  @GET
  @Path("/query")
  @ApiOperation(value = "Send query")
  public List<RootCauseEntity> query(
      @ApiParam(value = "framework name")
      @QueryParam("framework") String framework,
      @ApiParam(value = "start overall time window to consider for events")
      @QueryParam("analysisStart") Long analysisStart,
      @ApiParam(value = "end of overall time window to consider for events")
      @QueryParam("analysisEnd") Long analysisEnd,
      @ApiParam(value = "start time of the anomalous time period for the metric under analysis", defaultValue = "analysisStart")
      @QueryParam("anomalyStart") Long anomalyStart,
      @ApiParam(value = "end time of the anomalous time period for the metric under analysis", defaultValue = "analysisEnd")
      @QueryParam("anomalyEnd") Long anomalyEnd,
      @ApiParam(value = "baseline start time, e.g. anomaly start time offset by 1 week", defaultValue = "anomalyStart - 7 days")
      @QueryParam("baselineStart") Long baselineStart,
      @ApiParam(value = "baseline end time, e.g. typically anomaly start time offset by 1 week", defaultValue = "anomalyEnd - 7 days")
      @QueryParam("baselineEnd") Long baselineEnd,
      @QueryParam("formatterDepth") Integer formatterDepth,
      @ApiParam(value = "URNs of metrics to analyze")
      @QueryParam("urns") List<String> urns) throws Exception {

    // configuration validation
    if(!this.frameworks.containsKey(framework))
      throw new IllegalArgumentException(String.format("Could not resolve framework '%s'", framework));

    // input validation
    if(analysisStart == null)
      throw new IllegalArgumentException("Must provide analysis start timestamp (in milliseconds)");

    if(analysisEnd == null)
      throw new IllegalArgumentException("Must provide analysis end timestamp (in milliseconds)");

    if(anomalyStart == null)
      anomalyStart = analysisStart;

    if(anomalyEnd == null)
      anomalyEnd = analysisEnd;

    if(baselineStart == null)
      baselineStart = anomalyStart - TimeUnit.DAYS.toMillis(7);

    if(baselineEnd == null)
      baselineEnd = anomalyEnd - TimeUnit.DAYS.toMillis(7);

    if(formatterDepth == null)
      formatterDepth = DEFAULT_FORMATTER_DEPTH;

    if(analysisEnd - analysisStart > ANALYSIS_RANGE_MAX)
      throw new IllegalArgumentException(String.format("Analysis range cannot be longer than %d", ANALYSIS_RANGE_MAX));

    if(anomalyEnd - anomalyStart > ANOMALY_RANGE_MAX)
      throw new IllegalArgumentException(String.format("Anomaly range cannot be longer than %d", ANOMALY_RANGE_MAX));

    if(baselineEnd - baselineStart > BASELINE_RANGE_MAX)
      throw new IllegalArgumentException(String.format("Baseline range cannot be longer than %d", BASELINE_RANGE_MAX));

    urns = ResourceUtils.parseListParams(urns);

    // validate window size
    long anomalyWindow = anomalyEnd - anomalyStart;
    long baselineWindow = baselineEnd - baselineStart;
    if(anomalyWindow != baselineWindow)
      throw new IllegalArgumentException("Must provide equal-sized anomaly and baseline periods");

    // format input
    Set<Entity> input = new HashSet<>();
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANOMALY, anomalyStart, anomalyEnd));
    input.add(TimeRangeEntity.fromRange(0.8, TimeRangeEntity.TYPE_BASELINE, baselineStart, baselineEnd));
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANALYSIS, analysisStart, analysisEnd));
    for(String urn : urns) {
      input.add(EntityUtils.parseURN(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.frameworks.get(framework).run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted(), formatterDepth);
  }

  @GET
  @Path("/raw")
  @ApiOperation(value = "Raw")
  public List<RootCauseEntity> raw(
      @QueryParam("framework") String framework,
      @QueryParam("formatterDepth") Integer formatterDepth,
      @QueryParam("urns") List<String> urns) throws Exception {

    // configuration validation
    if(!this.frameworks.containsKey(framework))
      throw new IllegalArgumentException(String.format("Could not resolve framework '%s'", framework));

    if(formatterDepth == null)
      formatterDepth = DEFAULT_FORMATTER_DEPTH;

    // parse urns arg
    urns = ResourceUtils.parseListParams(urns);

    // format input
    Set<Entity> input = new HashSet<>();
    for(String urn : urns) {
      input.add(EntityUtils.parseURNRaw(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.frameworks.get(framework).run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted(), formatterDepth);
  }

  private List<RootCauseEntity> applyFormatters(Iterable<Entity> entities, int maxDepth) {
    List<RootCauseEntity> output = new ArrayList<>();
    for(Entity e : entities) {
      output.add(this.applyFormatters(e, maxDepth));
    }
    return output;
  }

  private RootCauseEntity applyFormatters(Entity e, int remainingDepth) {
    for(RootCauseEntityFormatter formatter : this.formatters) {
      if(formatter.applies(e)) {
        try {
          RootCauseEntity rce = formatter.format(e);

          if(remainingDepth > 1) {
            for (Entity re : e.getRelated()) {
              rce.addRelatedEntity(this.applyFormatters(re, remainingDepth - 1));
            }
          } else {
            // clear out any related entities added by the formatter by default
            rce.setRelatedEntities(Collections.<RootCauseEntity>emptyList());
          }

          return rce;
        } catch (Exception ex) {
          LOG.warn("Error applying formatter '{}'. Skipping.", formatter.getClass().getName(), ex);
        }
      }
    }
    throw new IllegalArgumentException(String.format("No formatter for Entity '%s'", e.getUrn()));
  }

}
