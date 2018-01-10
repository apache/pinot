package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.MultiDimensionalSummary;
import com.linkedin.thirdeye.client.diffsummary.costfunction.BalancedCostFunction;
import com.linkedin.thirdeye.client.diffsummary.costfunction.CostFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
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
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  @GET
  @Path(value = "/summary/autoDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummary(@QueryParam("dataset") String collection,
      @QueryParam("metric") String metric,
      @QueryParam("currentStart") long currentStartInclusive,
      @QueryParam("currentEnd") long currentEndExclusive,
      @QueryParam("baselineStart") long baselineStartInclusive,
      @QueryParam("baselineEnd") long baselineEndExclusive,
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
      Dimensions dimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        dimensions =
            MultiDimensionalSummary.sanitizeDimensions(new Dimensions(Utils.getSchemaDimensionNames(collection)));
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

      CostFunction costFunction = new BalancedCostFunction();
      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);
      MultiDimensionalSummary mdSummary =
          new MultiDimensionalSummary(CACHE_REGISTRY_INSTANCE.getQueryCache(), costFunction, dateTimeZone);

      response = mdSummary
          .buildSummary(collection, metric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
              baselineEndExclusive, dimensions, filterSetMap, summarySize, topDimensions, hierarchies, doOneSideError);

    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse(metric);
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  @GET
  @Path(value = "/summary/manualDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummaryManualDimensionOrder(@QueryParam("dataset") String collection,
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

      CostFunction costFunction = new BalancedCostFunction();
      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);
      MultiDimensionalSummary mdSummary =
          new MultiDimensionalSummary(CACHE_REGISTRY_INSTANCE.getQueryCache(), costFunction, dateTimeZone);

      response = mdSummary
          .buildSummary(collection, metric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
              baselineEndExclusive, dimensions, filterSets, summarySize, 0, Collections.<List<String>>emptyList(),
              doOneSideError);
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      response = SummaryResponse.buildNotAvailableResponse(metric);
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }
}
