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
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;


@Path(value = "/dashboard")
public class SummaryResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResource.class);
  private static final String DEFAULT_TIMEZONE_ID = "UTC";
  private static final String DEFAULT_TOP_DIMENSIONS = "4";
  private static final String DEFAULT_HIERARCHIES = "[]";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DEFAULT_ONE_SIDE_ERROR = "false";

  @GET
  @Path(value = "/summary/test")
  @Produces(MediaType.APPLICATION_JSON)
  public String getSummary() throws Exception {
    return buildSummary("thirdeyeKbmi", "mobilePageViews", DEFAULT_TIMEZONE_ID, new Long(1469628000000L), new Long(
        1470146400000L), "HOURS",
        "browserName,continent,countryCode,deviceName,environment,locale,osName,pageKey,service,sourceApp", 5, 3, "[]",
        false);
  }

  @GET
  @Path(value = "/summary/autoDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummary(@QueryParam("dataset") String collection,
      @QueryParam("metrics") String metric,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("baselineStart") Long baselineStart,
      @QueryParam("currentStart") Long currentStart,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("topDimensions") @DefaultValue(DEFAULT_TOP_DIMENSIONS) int topDimensions,
      @QueryParam("hierarchies") @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError) throws Exception {
    if (summarySize < 1) summarySize = 1;

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    olapClient.setCollection(collection);
    olapClient.setMetricName(metric);
    olapClient.setGroupByTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity));
    olapClient.setBaselineStartInclusive(new DateTime(baselineStart, DateTimeZone.forID(timeZone)));
    olapClient.setCurrentStartInclusive(new DateTime(currentStart, DateTimeZone.forID(timeZone)));

    Dimensions dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
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
      @QueryParam("metrics") String metric,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("baselineStart") Long baselineStart,
      @QueryParam("currentStart") Long currentStart,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("oneSideError") @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError) throws Exception {
    if (summarySize < 1) summarySize = 1;

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    olapClient.setCollection(collection);
    olapClient.setMetricName(metric);
    olapClient.setGroupByTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity));
    olapClient.setBaselineStartInclusive(new DateTime(baselineStart, DateTimeZone.forID(timeZone)));
    olapClient.setCurrentStartInclusive(new DateTime(currentStart, DateTimeZone.forID(timeZone)));

    Dimensions dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));

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
