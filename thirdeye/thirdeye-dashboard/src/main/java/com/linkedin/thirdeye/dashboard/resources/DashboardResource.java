package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.*;
import com.linkedin.thirdeye.dashboard.util.*;
import com.linkedin.thirdeye.dashboard.views.*;
import com.sun.jersey.api.NotFoundException;
import io.dropwizard.views.View;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardResource.class);
  private static final long INTRA_DAY_PERIOD = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
  private static final Joiner PATH_JOINER = Joiner.on("/");

  private final String serverUri;
  private final String feedbackEmailAddress;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;
  private final CustomDashboardResource customDashboardResource;

  public DashboardResource(String serverUri,
                           DataCache dataCache,
                           String feedbackEmailAddress,
                           QueryCache queryCache,
                           ObjectMapper objectMapper,
                           CustomDashboardResource customDashboardResource) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
    this.customDashboardResource = customDashboardResource;
    this.feedbackEmailAddress = feedbackEmailAddress;
  }

  @GET
  public Response getRoot() {
    return Response.seeOther(URI.create("/dashboard")).build();
  }

  @GET
  @Path("/dashboard")
  public LandingView getLandingView() throws Exception {
    List<String> collections = dataCache.getCollections(serverUri);
    if (collections.isEmpty()) {
      throw new NotFoundException("No collections loaded into " + serverUri);
    }
    return new LandingView(collections);
  }

  @GET
  @Path("/dashboard/{collection}")
  public DashboardStartView getDashboardStartView(@PathParam("collection") String collection) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);

    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(serverUri, collection);
    if (segments.isEmpty()) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
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

    return new DashboardStartView(collection, schema, earliestDataTime, latestDataTime, customDashboardNames);
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}")
  public Response getDashboardView(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @Context UriInfo uriInfo) throws Exception {
    return Response.seeOther(URI.create(PATH_JOINER.join(
        "",
        "dashboard",
        collection,
        metricFunction,
        MetricViewType.INTRA_DAY,
        DimensionViewType.HEAT_MAP))).build();
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}/{metricViewType}/{dimensionViewType}/{baselineOffsetMillis}")
  public View getDashboardView(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineOffsetMillis") Long baselineOffsetMillis,
      @Context UriInfo uriInfo) throws Exception {
    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(serverUri, collection);
    if (segments.isEmpty()) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
        latestDataTime = segment.getEndDataTime();
      }
    }

    if (earliestDataTime == null || latestDataTime == null) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    return getDashboardView(collection,
        metricFunction,
        metricViewType,
        dimensionViewType,
        latestDataTime.getMillis() - baselineOffsetMillis,
        latestDataTime.getMillis(),
        uriInfo);
  }

  @GET
  @Path("/dashboard/{collection}/{metricFunction}/{metricViewType}/{dimensionViewType}/{baselineMillis}/{currentMillis}")
  public View getDashboardView(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis,
      @Context UriInfo uriInfo) throws Exception {
    Map<String, String> dimensionValues = UriUtils.extractDimensionValues(uriInfo.getQueryParameters());

    // Check no group bys
    for (Map.Entry<String, String> entry : dimensionValues.entrySet()) {
      if ("!".equals(entry.getValue())) {
        throw new WebApplicationException(
            new IllegalArgumentException("No group by dimensions allowed"), Response.Status.BAD_REQUEST);
      }
    }

    // Get segment metadata
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(serverUri, collection);
    if (segments.isEmpty()) {
      throw new NotFoundException("No data loaded in server for " + collection);
    }

    // Find the latest and earliest data times
    DateTime earliestDataTime = null;
    DateTime latestDataTime = null;
    for (SegmentDescriptor segment : segments) {
      if (segment.getStartDataTime() != null && (earliestDataTime == null || segment.getStartDataTime().compareTo(earliestDataTime) < 0)) {
        earliestDataTime = segment.getStartDataTime();
      }
      if (segment.getEndDataTime() != null && (latestDataTime == null || segment.getEndDataTime().compareTo(latestDataTime) > 0)) {
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

    try {
      View metricView = getMetricView(
          collection, metricFunction, metricViewType, baselineMillis, currentMillis, uriInfo);

      View dimensionView = getDimensionView(
          collection, metricFunction, dimensionViewType, baselineMillis, currentMillis, uriInfo);

      return new DashboardView(
          collection,
          dataCache.getCollectionSchema(serverUri, collection),
          new DateTime(baselineMillis),
          new DateTime(currentMillis),
          new MetricView(metricView, metricViewType),
          new DimensionView(dimensionView, dimensionViewType), 
          earliestDataTime,
          latestDataTime,
          customDashboardNames,
          feedbackEmailAddress);
    } catch (Exception e) {
      if (e instanceof WebApplicationException) {
        throw e;  // sends appropriate HTTP response
      }

      // TODO: Better message, but at least this propagates it to client
      LOGGER.error("Error processing request {}", uriInfo.getRequestUri(), e);
      return new ExceptionView(e);
    }
  }

  @GET
  @Path("/metric/{collection}/{metricFunction}/{metricViewType}/{baselineMillis}/{currentMillis}")
  public View getMetricView(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis,
      @Context UriInfo uriInfo) throws Exception {
    MultivaluedMap<String, String> dimensionValues = uriInfo.getQueryParameters();
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);

    // Metric view
    switch (metricViewType) {
      case INTRA_DAY:
        String sql = SqlUtils.getSql(metricFunction, collection, new DateTime(baselineMillis - INTRA_DAY_PERIOD), new DateTime(currentMillis), dimensionValues);
        LOGGER.info("Generated SQL for {}: {}", uriInfo.getRequestUri(), sql);
        QueryResult result = queryCache.getQueryResult(serverUri, sql);
        return new MetricViewTabular(
            schema,
            objectMapper,
            result,
            currentMillis - baselineMillis,
            INTRA_DAY_PERIOD);
      case TIME_SERIES_FULL:
      case TIME_SERIES_OVERLAY:
      case FUNNEL:
        // n.b. will query /flot resource async
        return new MetricViewTimeSeries(schema, ViewUtils.flattenDisjunctions(dimensionValues));
      default:
        throw new NotFoundException("No metric view implementation for " + metricViewType);
    }
  }

  @GET
  @Path("/dimension/{collection}/{metricFunction}/{dimensionViewType}/{baselineMillis}/{currentMillis}")
  public View getDimensionView(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis,
      @Context UriInfo uriInfo) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);
    MultivaluedMap<String, String> dimensionValues = uriInfo.getQueryParameters();

    // Dimension view
    Map<String, Future<QueryResult>> resultFutures = new HashMap<>();
    switch (dimensionViewType) {
      case MULTI_TIME_SERIES:
        List<String> multiTimeSeriesDimensions = new ArrayList<>();
        for (String dimension : schema.getDimensions()) {
          if (!dimensionValues.containsKey(dimension)) {
            // Generate SQL (n.b. will query /flot resource async)
            dimensionValues.put(dimension, Arrays.asList("!"));
            String sql = SqlUtils.getSql(metricFunction, collection, baseline, current, dimensionValues);
            LOGGER.info("Generated SQL for {}: {}", uriInfo.getRequestUri(), sql);
            dimensionValues.remove(dimension);

            multiTimeSeriesDimensions.add(dimension);
          }
        }
        return new DimensionViewMultiTimeSeries(multiTimeSeriesDimensions);
      case HEAT_MAP:
        for (String dimension : schema.getDimensions()) {
          if (!dimensionValues.containsKey(dimension)) {
            // Generate SQL
            dimensionValues.put(dimension, Arrays.asList("!"));
            String sql = SqlUtils.getSql(metricFunction, collection, baseline, current, dimensionValues);
            LOGGER.info("Generated SQL for {}: {}", uriInfo.getRequestUri(), sql);
            dimensionValues.remove(dimension);

            // Query (in parallel)
            resultFutures.put(dimension, queryCache.getQueryResultAsync(serverUri, sql));
          }
        }
        // Wait for all queries
        Map<String, QueryResult> results = new HashMap<>(resultFutures.size());
        for (Map.Entry<String, Future<QueryResult>> entry : resultFutures.entrySet()) {
          results.put(entry.getKey(), entry.getValue().get());
        }
        return new DimensionViewHeatMap(schema, objectMapper, results);
      default:
        throw new NotFoundException("No dimension view implementation for " + dimensionViewType);
    }
  }
}
