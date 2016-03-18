package com.linkedin.thirdeye.dashboard.resources;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URIBuilder;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.client.ThirdEyeMetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.DimensionViewType;
import com.linkedin.thirdeye.dashboard.api.MetricViewType;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.QueryUtils;
import com.linkedin.thirdeye.dashboard.util.UriUtils;
import com.linkedin.thirdeye.dashboard.views.DashboardView;
import com.linkedin.thirdeye.dashboard.views.DimensionView;
import com.linkedin.thirdeye.dashboard.views.DimensionViewFunnel;
import com.linkedin.thirdeye.dashboard.views.DimensionViewHeatMap;
import com.linkedin.thirdeye.dashboard.views.ExceptionView;
import com.linkedin.thirdeye.dashboard.views.FunnelTable;
import com.sun.jersey.api.NotFoundException;

import io.dropwizard.views.View;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource {

  static final Logger LOGGER = LoggerFactory.getLogger(DashboardResource.class);

  private static final Joiner PATH_JOINER = Joiner.on("/");
  private static final int HEATMAP_GROUP_COUNT = 25;

  private final String feedbackEmailAddress;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;
  private final CustomDashboardResource customDashboardResource;
  private final FunnelsDataProvider funnelResource;
  private final ContributorDataProvider contributorResource;
  private final DashboardConfigResource dashboardConfigResource;

  private final ConfigCache configCache;

  public DashboardResource(DataCache dataCache, String feedbackEmailAddress, QueryCache queryCache,
      ObjectMapper objectMapper, CustomDashboardResource customDashboardResource,
      ConfigCache configCache, FunnelsDataProvider funnelResource,
      ContributorDataProvider contributorResource,
      DashboardConfigResource dashboardConfigResource) {
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
    this.customDashboardResource = customDashboardResource;
    this.feedbackEmailAddress = feedbackEmailAddress;
    this.configCache = configCache;
    this.funnelResource = funnelResource;
    this.contributorResource = contributorResource;
    this.dashboardConfigResource = dashboardConfigResource;
  }

  @GET
  public Response getRoot() {
    return Response.seeOther(URI.create("/dashboard")).build();
  }

  @GET
  @Path("/dashboard")
  public Response getLanding() throws Exception {
    List<String> collections = dataCache.getCollections();
    if (collections == null || collections.isEmpty()) {
      throw new NotFoundException("No collections loaded!");
    }
    return Response.seeOther(URI.create("/dashboard/" + collections.get(0))).build();
  }

  @GET
  @Path("/dashboard/{collection}/configs")
  public String getCollectionInfoJSON(@PathParam("collection") String collection) throws Exception {
    // TODO check if this is being used outside of dashboard (not used on main page)
    if (collection == null) {
      return null;
    }
    JSONObject ret = new JSONObject(
        new ObjectMapper().writeValueAsString(funnelResource.getFunnelSpecFor(collection)));
    JSONObject dimGroups = new JSONObject(
        new ObjectMapper().writeValueAsString(configCache.getDimensionGroupSpec(collection)));
    ret.put("dimension_groups", dimGroups.get("groups"));
    return ret.toString();
  }

  @GET
  @Path("/dashboard/{collection}")
  public Response getDashboardCollectionStart(@PathParam("collection") String collection)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);

    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(collection);
    if (segments.isEmpty()) {

      throw new NotFoundException("No data available for " + collection
          + ". Selectable collections are " + dataCache.getCollections());
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null
          || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null
          && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
        latestDataTime = segment.getEndDataTime();
      }
    }

    if (earliestDataTime == null || latestDataTime == null) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    List<String> funnelNames = Collections.emptyList();
    if (funnelResource != null) {
      funnelNames = funnelResource.getFunnelNamesFor(collection);
    }

    // Reflecting UI defaults: current time = latest - default bucket size (1 hour)
    long currentTimeMillis =
        latestDataTime.getMillis() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
    // Default baseline = current - 1 week
    long baselineTimeMillis = currentTimeMillis - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    String firstMetric = schema.getMetrics().get(0);
    String metricFunction = String.format("AGGREGATE_1_HOURS('%s')", firstMetric);

    String baseUrl = PATH_JOINER.join("/dashboard", collection, metricFunction,
        MetricViewType.INTRA_DAY, DimensionViewType.TABULAR, baselineTimeMillis, currentTimeMillis);
    String funnelsParam = Joiner.on(",").join(funnelNames);
    URI uri = new URIBuilder(baseUrl).addParameter("funnels", funnelsParam).build();
    return Response.seeOther(uri).build();
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}")
  public Response getDashboardView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction, @Context UriInfo uriInfo)
      throws Exception {
    // TODO check if this is being used outside of dashboard (not used on main page)
    return Response.seeOther(URI.create(PATH_JOINER.join("", "dashboard", collection,
        metricFunction, MetricViewType.INTRA_DAY, DimensionViewType.HEAT_MAP))).build();
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}/{metricViewType}/{dimensionViewType}/{baselineOffsetMillis}")
  public View getDashboardView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineOffsetMillis") Long baselineOffsetMillis, @Context UriInfo uriInfo)
      throws Exception {
    // TODO check if this is being used outside of dashboard (not used on main page)
    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(collection);
    if (segments.isEmpty()) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null
          || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null
          && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
        latestDataTime = segment.getEndDataTime();
      }
    }

    if (earliestDataTime == null || latestDataTime == null) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    return getDashboardView(collection, metricFunction, metricViewType, dimensionViewType,
        latestDataTime.getMillis() - baselineOffsetMillis, latestDataTime.getMillis(), uriInfo);
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}/{metricViewType}/{dimensionViewType}/{baselineMillis}/{currentMillis}")
  public View getDashboardView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis, @Context UriInfo uriInfo) throws Exception {
    Multimap<String, String> selectedDimensions = UriUtils.extractDimensionValues(uriInfo);
    Collection<String> selectedFunnelsList = UriUtils.extractFunnels(uriInfo);
    String selectedFunnels = (selectedFunnelsList == null || selectedFunnelsList.isEmpty()) ? null
        : selectedFunnelsList.iterator().next();

    // Check no group bys
    for (String value : selectedDimensions.values()) {
      if (ThirdEyeRequest.GROUP_BY_VALUE.equals(value)) {
        throw new WebApplicationException(
            new IllegalArgumentException("No group by dimensions allowed"),
            Response.Status.BAD_REQUEST);
      }
    }

    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(collection);
    if (segments.isEmpty()) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null
          || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null
          && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
        latestDataTime = segment.getEndDataTime();
      }
    }

    if (earliestDataTime == null || latestDataTime == null) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Any custom dashboards that are defined
    List<String> customDashboardNames = null;
    if (customDashboardResource != null) {
      customDashboardNames = customDashboardResource.getCustomDashboardNames(collection);
    }

    // lets find if there are any funnels

    List<String> funnelNames = Collections.emptyList();
    if (funnelResource != null) {
      funnelNames = funnelResource.getFunnelNamesFor(collection);
    }

    try {
      View dimensionView = getDimensionView(collection, metricFunction, dimensionViewType,
          baselineMillis, currentMillis, selectedDimensions, selectedFunnels, uriInfo);

      Map<String, Collection<String>> dimensionValueOptions =
          dashboardConfigResource.getDimensionValues(collection, baselineMillis, currentMillis);

      return new DashboardView(collection, selectedDimensions, new DateTime(baselineMillis),
          new DateTime(currentMillis),
          new DimensionView(dimensionView, dimensionViewType, dimensionValueOptions),
          earliestDataTime, latestDataTime, customDashboardNames, feedbackEmailAddress, funnelNames,
          dashboardConfigResource);
    } catch (WebApplicationException e) {
      throw e;// sends appropriate HTTP response
    } catch (Exception e) {
      // TODO: Better message, but at least this propagates it to client
      LOGGER.error("Error processing request {}", uriInfo.getRequestUri(), e);
      return new ExceptionView(e);
    }
  }

  @GET
  @Path("/dimension/{collection}/{metricFunction}/{dimensionViewType}/{baselineMillis}/{currentMillis}")
  public View getDimensionView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis, @Context UriInfo uriInfo) throws Exception {
    // TODO may be useful for refactor later.
    return getDimensionView(collection, metricFunction, dimensionViewType, baselineMillis,
        currentMillis, UriUtils.extractDimensionValues(uriInfo), null, uriInfo);
  }

  private View getDimensionView(String collection, String metricFunction,
      DimensionViewType dimensionViewType, Long baselineMillis, Long currentMillis,
      Multimap<String, String> selectedDimensions, String funnels, UriInfo uriInfo)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);
    LOGGER.info("Request uri: {}", uriInfo.getRequestUri());
    // Dimension groups
    Map<String, Multimap<String, String>> reverseDimensionGroups = null;
    DimensionGroupSpec dimensionGroupSpec = configCache.getDimensionGroupSpec(collection);
    if (dimensionGroupSpec != null) {
      reverseDimensionGroups = dimensionGroupSpec.getReverseMapping();
    }

    // Dimension view
    switch (dimensionViewType) {
    case CONTRIBUTOR:
    case MULTI_TIME_SERIES:
      List<String> viewDimensions = dashboardConfigResource.getDimensions(collection, uriInfo);
      return contributorResource.generateDimensionContributorView(collection, metricFunction,
          selectedDimensions, viewDimensions, baseline, current, reverseDimensionGroups);

    case HEAT_MAP:
      Map<String, List<Future<QueryResult>>> resultActualFutures = new HashMap<>();
      // TODO separate this into two requests for the current + baseline period only (eg 2 days
      // instead of a full week)
      Multimap<String, String> expandedDimensionValues =
          ThirdEyeRequestUtils.expandDimensionGroups(selectedDimensions, reverseDimensionGroups);

      ThirdEyeMetricFunction thirdEyeMetricFunction =
          ThirdEyeMetricFunction.fromStr(metricFunction);
      long bucketSize = thirdEyeMetricFunction.getTimeGranularity().toMillis();
      DateTime baselineStart = baseline;
      DateTime baselineEnd = baseline.plus(bucketSize);
      DateTime currentStart = current;
      DateTime currentEnd = current.plus(bucketSize);

      for (String dimension : schema.getDimensions()) {
        if (!selectedDimensions.containsKey(dimension)) {
          // Generate SQL
          ThirdEyeRequest baseLineReq = new ThirdEyeRequestBuilder().setCollection(collection)
              .setMetricFunction(thirdEyeMetricFunction).setDimensionValues(expandedDimensionValues)
              .setGroupBy(dimension).setStartTimeInclusive(baselineStart).setEndTime(baselineEnd)
              .setTopCount(HEATMAP_GROUP_COUNT).build();

          ThirdEyeRequest currentReq = new ThirdEyeRequestBuilder().setCollection(collection)
              .setMetricFunction(thirdEyeMetricFunction).setDimensionValues(expandedDimensionValues)
              .setGroupBy(dimension).setTopCount(HEATMAP_GROUP_COUNT).setStartTimeInclusive(currentStart)
              .setEndTime(currentEnd).build();

          // Query (in parallel)
          ArrayList<Future<QueryResult>> futures = new ArrayList<>();
          futures.add(queryCache.getQueryResultAsync(baseLineReq));
          futures.add(queryCache.getQueryResultAsync(currentReq));
          LOGGER.info("Generated request for heat map, dimension: {}, request: {}", dimension,
              baseLineReq);
          LOGGER.info("Generated request for heat map: dimension: {}, request: {}", dimension,
              currentReq);
          resultActualFutures.put(dimension, futures);
        }
      }

      List<Future<QueryResult>> resultFuturesForTotal = new ArrayList<>(2);

      ThirdEyeRequest baselineReqForTotal = new ThirdEyeRequestBuilder().setCollection(collection)
          .setMetricFunction(thirdEyeMetricFunction).setDimensionValues(expandedDimensionValues)
          .setStartTimeInclusive(baselineStart).setEndTime(baselineEnd).build();

      ThirdEyeRequest currentReqForTotal = new ThirdEyeRequestBuilder().setCollection(collection)
          .setMetricFunction(thirdEyeMetricFunction).setDimensionValues(expandedDimensionValues)
          .setStartTimeInclusive(currentStart).setEndTime(currentEnd).build();

      LOGGER.info("Generated Total request for heat map: {}", baselineReqForTotal);
      LOGGER.info("Generated Total request for heat map: {}", currentReqForTotal);

      resultFuturesForTotal.add(queryCache.getQueryResultAsync(baselineReqForTotal));
      resultFuturesForTotal.add(queryCache.getQueryResultAsync(currentReqForTotal));

      // Get the possible dimension groups
      Map<String, Map<String, String>> dimensionGroups = null;
      Map<String, Map<Pattern, String>> dimensionRegex = null;
      DimensionGroupSpec groupSpec = configCache.getDimensionGroupSpec(collection);
      if (groupSpec != null) {
        dimensionGroups = groupSpec.getMapping();
        dimensionRegex = groupSpec.getRegexMapping();
      }

      // Wait for all queries
      Map<String, QueryResult> actualResults =
          QueryUtils.waitForAndMergeMultipleResults(resultActualFutures);

      QueryResult resultForTotal = QueryUtils.waitForAndMergeMultipleResults(resultFuturesForTotal);

      DimensionViewHeatMap dimensionViewHeatMap = new DimensionViewHeatMap(schema, objectMapper,
          actualResults, dimensionGroups, dimensionRegex, baseline, current, resultForTotal);
      return dimensionViewHeatMap;
    case TABULAR:
      List<FunnelTable> funnelTables = funnelResource.computeFunnelViews(collection, metricFunction,
          funnels, baselineMillis, currentMillis, selectedDimensions);
      return new DimensionViewFunnel(funnelTables);
    default:
      throw new NotFoundException("No dimension view implementation for " + dimensionViewType);
    }

  }

}
