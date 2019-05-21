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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.cube.additive.AdditiveDBClient;
import org.apache.pinot.thirdeye.cube.cost.BalancedCostFunction;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.cost.OeRatioCostFunction;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalRatioSummary;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalSummary;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalSummaryCLITool;
import org.apache.pinot.thirdeye.cube.ratio.RatioDBClient;
import org.apache.pinot.thirdeye.cube.summary.SummaryResponse;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/dashboard")
public class SummaryResource {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private static final String DEFAULT_TIMEZONE_ID = "UTC";
  private static final String DEFAULT_DEPTH = "3";
  private static final String DEFAULT_HIERARCHIES = "[]";
  private static final String DEFAULT_ONE_SIDE_ERROR = "false";
  private static final String DEFAULT_EXCLUDED_DIMENSIONS = "";
  private static final String JAVASCRIPT_NULL_STRING = "undefined";
  private static final String HTML_STRING_ENCODING = "UTF-8";

  @GET
  @Path(value = "/summary/autoDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummary(@QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam("currentStart") long currentStartInclusive,
      @QueryParam("currentEnd") long currentEndExclusive,
      @QueryParam("baselineStart") long baselineStartInclusive,
      @QueryParam("baselineEnd") long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth,
      @QueryParam("hierarchies") @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("excludedDimensions") @DefaultValue(DEFAULT_EXCLUDED_DIMENSIONS) String excludedDimensions,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    SummaryResponse response = null;
    try {
      Dimensions dimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        dimensions =
            MultiDimensionalSummaryCLITool.sanitizeDimensions(new Dimensions(Utils.getSchemaDimensionNames(dataset)));
      } else {
        dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
      }

      if (!Strings.isNullOrEmpty(excludedDimensions)) {
        List<String> dimensionsToBeRemoved = Arrays.asList(excludedDimensions.trim().split(","));
        dimensions = MultiDimensionalSummaryCLITool.removeDimensions(dimensions, dimensionsToBeRemoved);
      }

      Multimap<String, String> filterSetMap;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSetMap = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSetMap = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      List<List<String>> hierarchies =
          OBJECT_MAPPER.readValue(hierarchiesPayload, new TypeReference<List<List<String>>>() {
          });

      CostFunction costFunction = new BalancedCostFunction();
      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);
      AdditiveDBClient cubeDbClient = new AdditiveDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      MultiDimensionalSummary mdSummary = new MultiDimensionalSummary(cubeDbClient, costFunction, dateTimeZone);

      response = mdSummary
          .buildSummary(dataset, metric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
              baselineEndExclusive, dimensions, filterSetMap, summarySize, depth, hierarchies, doOneSideError);

    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse(dataset, metric);
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  @GET
  @Path(value = "/summary/manualDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummaryManualDimensionOrder(@QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam("currentStart") long currentStartInclusive,
      @QueryParam("currentEnd") long currentEndExclusive,
      @QueryParam("baselineStart") long baselineStartInclusive,
      @QueryParam("baselineEnd") long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    SummaryResponse response = null;
    try {
      List<String> allDimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        allDimensions = Utils.getSchemaDimensionNames(dataset);
      } else {
        allDimensions = Arrays.asList(groupByDimensions.trim().split(","));
      }
      if (allDimensions.size() > Integer.parseInt(DEFAULT_DEPTH)) {
        allDimensions = allDimensions.subList(0, Integer.parseInt(DEFAULT_DEPTH));
      }
      Dimensions dimensions = new Dimensions(allDimensions);

      Multimap<String, String> filterSets;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSets = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSets = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      CostFunction costFunction = new BalancedCostFunction();
      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);
      AdditiveDBClient cubeDbClient = new AdditiveDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      MultiDimensionalSummary mdSummary = new MultiDimensionalSummary(cubeDbClient, costFunction, dateTimeZone);

      response = mdSummary
          .buildSummary(dataset, metric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
              baselineEndExclusive, dimensions, filterSets, summarySize, 0, Collections.<List<String>>emptyList(),
              doOneSideError);
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse(dataset, metric);
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  @GET
  @Path(value = "/summary/autoRatioDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildRatioSummary(@QueryParam("dataset") String dataset,
      @QueryParam("numeratorMetric") String numeratorMetric,
      @QueryParam("denominatorMetric") String denominatorMetric,
      @QueryParam("currentStart") long currentStartInclusive,
      @QueryParam("currentEnd") long currentEndExclusive,
      @QueryParam("baselineStart") long baselineStartInclusive,
      @QueryParam("baselineEnd") long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth,
      @QueryParam("hierarchies") @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("excludedDimensions") @DefaultValue(DEFAULT_EXCLUDED_DIMENSIONS) String excludedDimensions,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    SummaryResponse response = null;

    try {
      Dimensions dimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        dimensions =
            MultiDimensionalSummaryCLITool.sanitizeDimensions(new Dimensions(Utils.getSchemaDimensionNames(dataset)));
      } else {
        dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
      }

      if (!Strings.isNullOrEmpty(excludedDimensions)) {
        List<String> dimensionsToBeRemoved = Arrays.asList(excludedDimensions.trim().split(","));
        dimensions = MultiDimensionalSummaryCLITool.removeDimensions(dimensions, dimensionsToBeRemoved);
      }

      Multimap<String, String> filterSetMap;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSetMap = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSetMap = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      List<List<String>> hierarchies =
          OBJECT_MAPPER.readValue(hierarchiesPayload, new TypeReference<List<List<String>>>() {
          });

      CostFunction costFunction = new OeRatioCostFunction();
      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);
      RatioDBClient dbClient = new RatioDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      MultiDimensionalRatioSummary mdSummary = new MultiDimensionalRatioSummary(dbClient, costFunction, dateTimeZone);

      response = mdSummary
          .buildRatioSummary(dataset, numeratorMetric, denominatorMetric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
              baselineEndExclusive, dimensions, filterSetMap, summarySize, depth, hierarchies, doOneSideError);

    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse(dataset, numeratorMetric + "/" + denominatorMetric);
    }

    return OBJECT_MAPPER.writeValueAsString(response);
  }
}
