package com.linkedin.thirdeye.dashboard.resources;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;
import com.linkedin.thirdeye.util.ThirdEyeUtils;


@Path(value = "/dashboard")
public class SummaryResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private static final Logger LOG = LoggerFactory.getLogger(SummaryResource.class);
  private static final String DEFAULT_TIMEZONE_ID = "UTC";
  private static final String DEFAULT_TOP_DIMENSIONS = "3";
  private static final String DEFAULT_HIERARCHIES = "[]";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DEFAULT_ONE_SIDE_ERROR = "false";

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
      @QueryParam("summarySize") int summarySize,
      @QueryParam("topDimensions") @DefaultValue(DEFAULT_TOP_DIMENSIONS) int topDimensions,
      @QueryParam("hierarchies") @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    collection = ThirdEyeUtils.getCollectionFromAlias(collection);
    CollectionConfig collectionConfig = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().getIfPresent(collection);
    if (collectionConfig != null && collectionConfig.getDerivedMetrics() != null
        && collectionConfig.getDerivedMetrics().containsKey(metric)) {
      metric = collectionConfig.getDerivedMetrics().get(metric);
    }
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, collection);

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    olapClient.setCollection(collection);
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, DateTimeZone.forID(timeZone)));
    olapClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, DateTimeZone.forID(timeZone)));
    olapClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, DateTimeZone.forID(timeZone)));
    olapClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, DateTimeZone.forID(timeZone)));

    Dimensions dimensions;
    if (groupByDimensions == null || groupByDimensions.length() == 0 || groupByDimensions.equals("undefined")) {
      dimensions = new Dimensions(Utils.getDimensions(CACHE_REGISTRY_INSTANCE.getQueryCache(), collection));
    } else {
      dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
    }

    List<List<String>> hierarchies =
        OBJECT_MAPPER.readValue(hierarchiesPayload, new TypeReference<List<List<String>>>() {
        });

    SummaryResponse response = null;
    try {
        Cube cube = new Cube();
        cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, hierarchies);

        Summary summary = new Summary(cube);
        response = summary.computeSummary(summarySize, doOneSideError, topDimensions);
        summary.testCorrectnessOfWowValues();
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
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
      @QueryParam("summarySize") int summarySize,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    collection = ThirdEyeUtils.getCollectionFromAlias(collection);
    CollectionConfig collectionConfig = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().getIfPresent(collection);
    if (collectionConfig != null && collectionConfig.getDerivedMetrics() != null
        && collectionConfig.getDerivedMetrics().containsKey(metric)) {
      metric = collectionConfig.getDerivedMetrics().get(metric);
    }
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, collection);

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    olapClient.setCollection(collection);
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, DateTimeZone.forID(timeZone)));
    olapClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, DateTimeZone.forID(timeZone)));
    olapClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, DateTimeZone.forID(timeZone)));
    olapClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, DateTimeZone.forID(timeZone)));

    List<String> allDimensions;
    if (groupByDimensions == null || groupByDimensions.length() == 0 || groupByDimensions.equals("undefined")) {
      allDimensions = Utils.getDimensions(CACHE_REGISTRY_INSTANCE.getQueryCache(), collection);
    } else {
      allDimensions = Arrays.asList(groupByDimensions.trim().split(","));
    }
    if (allDimensions.size() > Integer.parseInt(DEFAULT_TOP_DIMENSIONS)) {
      allDimensions = allDimensions.subList(0, Integer.parseInt(DEFAULT_TOP_DIMENSIONS));
    }
    Dimensions dimensions = new Dimensions(allDimensions);

    SummaryResponse response = null;
    try {
      Cube cube = new Cube();
      cube.buildWithManualDimensionOrder(olapClient, dimensions);

      Summary summary = new Summary(cube);
      response = summary.computeSummary(summarySize, doOneSideError);
      summary.testCorrectnessOfWowValues();
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
    }

    return OBJECT_MAPPER.writeValueAsString(response);
  }
}
