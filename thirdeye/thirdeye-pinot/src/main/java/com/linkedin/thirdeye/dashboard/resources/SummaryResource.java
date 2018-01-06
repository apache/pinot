package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.diffsummary.CostFunction;
import com.linkedin.thirdeye.constant.MetricAggFunction;

import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;


@Path(value = "/dashboard")
public class SummaryResource {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private static final String DEFAULT_TIMEZONE_ID = "UTC";
  private static final String DEFAULT_TOP_DIMENSIONS = "3";
  private static final String DEFAULT_HIERARCHIES = "[]";
  private static final String DEFAULT_ONE_SIDE_ERROR = "false";
  private static final String JAVASCRIPT_NULL_STRING = "undefined";
  private static final String HTML_STRING_ENCODING = "UTF-8";

  private static final String TOP_K_POSTFIX = "_topk";


  @GET
  @Path(value = "/summary/autoDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummary(@QueryParam("dataset") String collection,
      @QueryParam("metric") String metric,
      @QueryParam("currentStart") Long currentStartInclusive,
      @QueryParam("currentEnd") Long currentEndExclusive,
      @QueryParam("baselineStart") Long baselineStartInclusive,
      @QueryParam("baselineEnd") Long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("topDimensions") @DefaultValue(DEFAULT_TOP_DIMENSIONS) int topDimensions,
      @QueryParam("hierarchies") @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    SummaryResponse response = null;
    try {
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM,collection);

      OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      olapClient.setCollection(collection);
      olapClient.setMetricExpression(metricExpressions.get(0));
      olapClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, DateTimeZone.forID(timeZone)));
      olapClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, DateTimeZone.forID(timeZone)));
      olapClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, DateTimeZone.forID(timeZone)));
      olapClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, DateTimeZone.forID(timeZone)));

      Dimensions dimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        dimensions = sanitizeDimensions(new Dimensions(Utils.getSchemaDimensionNames(collection)));
      } else {
        dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
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

      CostFunction costFunction = new CostFunction();
      Cube cube = new Cube(costFunction);
      cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, hierarchies, filterSetMap);

      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError, topDimensions);
      response.setMetricName(metric);
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse();
      response.setMetricName(metric);
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  @GET
  @Path(value = "/summary/manualDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummaryManualDimensionOrder(@QueryParam("dataset") String collection,
      @QueryParam("metric") String metric,
      @QueryParam("currentStart") Long currentStartInclusive,
      @QueryParam("currentEnd") Long currentEndExclusive,
      @QueryParam("baselineStart") Long baselineStartInclusive,
      @QueryParam("baselineEnd") Long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    SummaryResponse response = null;
    try {
      List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, collection);

      OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      olapClient.setCollection(collection);
      olapClient.setMetricExpression(metricExpressions.get(0));
      olapClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, DateTimeZone.forID(timeZone)));
      olapClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, DateTimeZone.forID(timeZone)));
      olapClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, DateTimeZone.forID(timeZone)));
      olapClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, DateTimeZone.forID(timeZone)));

      List<String> allDimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        allDimensions = Utils.getSchemaDimensionNames(collection);
      } else {
        allDimensions = Arrays.asList(groupByDimensions.trim().split(","));
      }
      if (allDimensions.size() > Integer.parseInt(DEFAULT_TOP_DIMENSIONS)) {
        allDimensions = allDimensions.subList(0, Integer.parseInt(DEFAULT_TOP_DIMENSIONS));
      }
      Dimensions dimensions = new Dimensions(allDimensions);

      Multimap<String, String> filterSets;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSets = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSets = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      CostFunction costFunction = new CostFunction();
      Cube cube = new Cube(costFunction);
      cube.buildWithManualDimensionOrder(olapClient, dimensions, filterSets);

      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError);
      response.setMetricName(metric);
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse();
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  /**
   * Removes noisy dimensions.
   *
   * @param dimensions the original dimensions.
   *
   * @return the original dimensions minus noisy dimensions, which are predefined.
   *
   * TODO: Replace with an user configurable method
   */
  private static Dimensions sanitizeDimensions(Dimensions dimensions) {
    List<String> allDimensionNames = dimensions.allDimensions();
    Set<String> dimensionsToRemove = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    dimensionsToRemove.add("environment");
    dimensionsToRemove.add("colo");
    dimensionsToRemove.add("fabric");
    for (String dimensionName : allDimensionNames) {
      if(dimensionName.contains(TOP_K_POSTFIX)) {
        String rawDimensionName = dimensionName.replaceAll(TOP_K_POSTFIX, "");
        dimensionsToRemove.add(rawDimensionName.toLowerCase());
      }
    }
    return removeDimensions(dimensions, dimensionsToRemove);
  }

  private static Dimensions removeDimensions(Dimensions dimensions, Collection<String> dimensionsToRemove) {
    List<String> dimensionsToRetain = new ArrayList<>();
    for (String dimensionName : dimensions.allDimensions()) {
      if(!dimensionsToRemove.contains(dimensionName)){
        dimensionsToRetain.add(dimensionName);
      }
    }
    return new Dimensions(dimensionsToRetain);
  }
}
