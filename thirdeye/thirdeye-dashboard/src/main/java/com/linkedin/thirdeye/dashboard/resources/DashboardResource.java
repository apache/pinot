package com.linkedin.thirdeye.dashboard.resources;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.DimensionViewType;
import com.linkedin.thirdeye.dashboard.api.MetricViewType;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.api.SegmentDescriptor;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import com.linkedin.thirdeye.dashboard.views.DashboardStartView;
import com.linkedin.thirdeye.dashboard.views.DashboardView;
import com.linkedin.thirdeye.dashboard.views.DimensionView;
import com.linkedin.thirdeye.dashboard.views.DimensionViewFunnel;
import com.linkedin.thirdeye.dashboard.views.DimensionViewHeatMap;
import com.linkedin.thirdeye.dashboard.views.DimensionViewMultiTimeSeries;
import com.linkedin.thirdeye.dashboard.views.ExceptionView;
import com.linkedin.thirdeye.dashboard.views.FunnelTable;
import com.linkedin.thirdeye.dashboard.views.LandingView;
import com.linkedin.thirdeye.dashboard.views.MetricView;
import com.linkedin.thirdeye.dashboard.views.MetricViewTabular;
import com.linkedin.thirdeye.dashboard.views.MetricViewTimeSeries;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import io.dropwizard.views.View;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardResource.class);
  private static final long INTRA_DAY_PERIOD = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
  private static final long INTRA_WEEK_PERIOD = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
  private static final long INTRA_MONTH_PERIOD = TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);

  private static final String DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION = "AGGREGATE_1_HOURS(%s)";
  private static final double DIMENSION_VALUES_OPTIONS_THRESHOLD = 0.01;
  private static final int DIMENSION_VALUES_LIMIT = 25;
  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };
  private static final Joiner PATH_JOINER = Joiner.on("/");

  private final String serverUri;
  private final String feedbackEmailAddress;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;
  private final CustomDashboardResource customDashboardResource;
  private final FunnelsDataProvider funnelResource;

  private final ConfigCache configCache;

  public DashboardResource(String serverUri, DataCache dataCache, String feedbackEmailAddress,
      QueryCache queryCache, ObjectMapper objectMapper,
      CustomDashboardResource customDashboardResource, ConfigCache configCache,
      FunnelsDataProvider funnelResource) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
    this.customDashboardResource = customDashboardResource;
    this.feedbackEmailAddress = feedbackEmailAddress;
    this.configCache = configCache;
    this.funnelResource = funnelResource;
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
  public DashboardStartView getDashboardStartView(@PathParam("collection") String collection)
      throws Exception {
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

    List<String> funnelNames = Collections.emptyList();
    if (funnelResource != null) {
      funnelNames = funnelResource.getFunnelNamesFor(collection);
    }
    return new DashboardStartView(collection, schema, earliestDataTime, latestDataTime,
        customDashboardNames, funnelNames);
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
    List<SegmentDescriptor> segments = dataCache.getSegmentDescriptors(serverUri, collection);
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
    MultivaluedMap<String, String> selectedDimensions = uriInfo.getQueryParameters();
    List<String> selectedFunnelsList = selectedDimensions.remove("funnels");
    String selectedFunnels = (selectedFunnelsList == null || selectedFunnelsList.isEmpty()) ? null
        : selectedFunnelsList.get(0);

    // Check no group bys
    for (Entry<String, List<String>> entry : selectedDimensions.entrySet()) {
      for (String value : entry.getValue()) {
        if ("!".equals(value)) {
          throw new WebApplicationException(
              new IllegalArgumentException("No group by dimensions allowed"),
              Response.Status.BAD_REQUEST);
        }
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
      View metricView = null;
      if (dimensionViewType == DimensionViewType.MULTI_TIME_SERIES) {
        // TODO Follow up on multi-time-series.ftl, which is the only user of the metric view in the
        // intra_day dashboard.
        metricView = getMetricView(collection, metricFunction, metricViewType, baselineMillis,
            currentMillis, uriInfo);
      }

      View dimensionView = getDimensionView(collection, metricFunction, dimensionViewType,
          baselineMillis, currentMillis, selectedDimensions, selectedFunnels, uriInfo);

      Map<String, Collection<String>> dimensionValueOptions =
          retrieveDimensionValues(collection, baselineMillis, currentMillis);

      return new DashboardView(collection, dataCache.getCollectionSchema(serverUri, collection),
          selectedDimensions, new DateTime(baselineMillis), new DateTime(currentMillis),
          new MetricView(metricView, metricViewType),
          new DimensionView(dimensionView, dimensionViewType, dimensionValueOptions),
          earliestDataTime, latestDataTime, customDashboardNames, feedbackEmailAddress,
          funnelNames);
    } catch (Exception e) {
      if (e instanceof WebApplicationException) {
        throw e; // sends appropriate HTTP response
      }

      // TODO: Better message, but at least this propagates it to client
      LOGGER.error("Error processing request {}", uriInfo.getRequestUri(), e);
      return new ExceptionView(e);
    }
  }

  @GET
  @Path("/metric/{collection}/{metricFunction}/{metricViewType}/{baselineMillis}/{currentMillis}")
  public View getMetricView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("metricViewType") MetricViewType metricViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis, @Context UriInfo uriInfo) throws Exception {
    MultivaluedMap<String, String> dimensionValues = uriInfo.getQueryParameters();
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    // TODO check if anything is using this endpoint. Right now metricViewType is always INTRA_DAY
    // and is only useful when viewing the multi-time-series.
    // Metric view
    switch (metricViewType) {
    case INTRA_DAY:
      long intraPeriod = getIntraPeriod(metricFunction);

      // Dimension groups
      Map<String, Map<String, List<String>>> dimensionGroups = null;
      DimensionGroupSpec dimensionGroupSpec = configCache.getDimensionGroupSpec(collection);
      if (dimensionGroupSpec != null) {
        dimensionGroups = dimensionGroupSpec.getReverseMapping();
      }
      String sql =
          SqlUtils.getSql(metricFunction, collection, new DateTime(baselineMillis - intraPeriod),
              new DateTime(currentMillis), dimensionValues, dimensionGroups);
      LOGGER.info("Generated SQL for intraday metric {}: {}", uriInfo.getRequestUri(), sql);
      QueryResult result = queryCache.getQueryResult(serverUri, sql);

      return new MetricViewTabular(schema, objectMapper, result, currentMillis,
          currentMillis - baselineMillis, intraPeriod);

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
  public View getDimensionView(@PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("dimensionViewType") DimensionViewType dimensionViewType,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis, @Context UriInfo uriInfo) throws Exception {
    // TODO may be useful for refactor later.
    return getDimensionView(collection, metricFunction, dimensionViewType, baselineMillis,
        currentMillis, uriInfo.getQueryParameters(), null, uriInfo);
  }

  private View getDimensionView(String collection, String metricFunction,
      DimensionViewType dimensionViewType, Long baselineMillis, Long currentMillis,
      MultivaluedMap<String, String> selectedDimensions, String funnels, UriInfo uriInfo)
          throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);

    // Dimension groups
    Map<String, Map<String, List<String>>> reverseDimensionGroups = null;
    DimensionGroupSpec dimensionGroupSpec = configCache.getDimensionGroupSpec(collection);
    if (dimensionGroupSpec != null) {
      reverseDimensionGroups = dimensionGroupSpec.getReverseMapping();
    }

    // Dimension view
    switch (dimensionViewType) {
    case MULTI_TIME_SERIES:
      // determine time series dimensions to asynchronously query.
      // note: this order isn't sorted alphabetically...
      return new DimensionViewMultiTimeSeries(schema.getDimensions());
    case HEAT_MAP:

      Map<String, Future<QueryResult>> resultFutures = new HashMap<>();
      for (String dimension : schema.getDimensions()) {
        if (!selectedDimensions.containsKey(dimension)) {
          // Generate SQL
          selectedDimensions.put(dimension, Arrays.asList("!"));
          String sql = SqlUtils.getSql(metricFunction, collection, baseline, current,
              selectedDimensions, reverseDimensionGroups);
          LOGGER.info("Generated SQL for heat map {}: {}", uriInfo.getRequestUri(), sql);
          selectedDimensions.remove(dimension);

          // Query (in parallel)
          resultFutures.put(dimension, queryCache.getQueryResultAsync(serverUri, sql));
        }
      }

      // Get the possible dimension groups
      Map<String, Map<String, String>> dimensionGroups = null;
      Map<String, Map<Pattern, String>> dimensionRegex = null;
      DimensionGroupSpec groupSpec = configCache.getDimensionGroupSpec(collection);
      if (groupSpec != null) {
        dimensionGroups = groupSpec.getMapping();
        dimensionRegex = groupSpec.getRegexMapping();
      }

      // Wait for all queries
      Map<String, QueryResult> results = new HashMap<>(resultFutures.size());
      for (Map.Entry<String, Future<QueryResult>> entry : resultFutures.entrySet()) {
        results.put(entry.getKey(), entry.getValue().get());
      }

      return new DimensionViewHeatMap(schema, objectMapper, results, dimensionGroups,
          dimensionRegex);
    case TABULAR:
      List<FunnelTable> funnelTables =
          funnelResource.computeFunnelViews(collection, metricFunction, funnels, baselineMillis,
              currentMillis, getIntraPeriod(metricFunction), selectedDimensions);
      return new DimensionViewFunnel(funnelTables);
    default:
      throw new NotFoundException("No dimension view implementation for " + dimensionViewType);
    }

  }

  /**
   * Determines intra period (day/week/month) based on outermost metric function.
   * @param metricFunction
   * @return
   * @throws NumberFormatException
   */
  private long getIntraPeriod(String metricFunction) throws NumberFormatException {
    String timeUnit = metricFunction.split("_")[2];
    int aggregationWindow = Integer.parseInt(metricFunction.split("_")[1]);

    long intraPeriod = INTRA_DAY_PERIOD;
    if (timeUnit.startsWith(TimeUnit.DAYS.toString()) && aggregationWindow == 1) {
      intraPeriod = INTRA_WEEK_PERIOD;
    } else if (timeUnit.startsWith(TimeUnit.DAYS.toString())) {
      intraPeriod = INTRA_MONTH_PERIOD;
    }
    return intraPeriod;
  }

  /**
   * Returns a map of dimensions -> list of observed dimension values in the given time
   * window, sorted in decreasing order of overall contribution. Any items not meeting a
   * contribution threshold of {@value #DIMENSION_VALUES_OPTIONS_THRESHOLD} are omitted, and a
   * maximum of {@value #DIMENSION_VALUES_LIMIT} results are returned for any given dimension.
   */
  @Deprecated
  private Map<String, Collection<String>> retrieveDimensionValues(String collection,
      long baselineMillis, long currentMillis) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);

    String firstMetric = schema.getMetrics().get(0);
    String dummyFunction = String.format(DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION, firstMetric);
    // query w/ group by for each dimension.
    List<String> dimensions = schema.getDimensions();
    MultivaluedMap<String, String> dimensionValues = new MultivaluedMapImpl();
    Map<String, Future<QueryResult>> resultFutures = new HashMap<>();
    for (String dimension : schema.getDimensions()) {
      // Generate SQL
      dimensionValues.put(dimension, Arrays.asList("!"));
      String sql =
          SqlUtils.getSql(dummyFunction, collection, baseline, current, dimensionValues, null);
      LOGGER.info("Generated SQL for dimension retrieval {}: {}", serverUri, sql);
      dimensionValues.remove(dimension);

      // Query (in parallel)
      resultFutures.put(dimension, queryCache.getQueryResultAsync(serverUri, sql));
    }

    Map<String, Collection<String>> collectedDimensionValues = new HashMap<>();
    // Wait for all queries and generate the ordered list from the result.
    for (int i = 0; i < dimensions.size(); i++) {
      String dimension = dimensions.get(i);
      QueryResult queryResult = resultFutures.get(dimension).get();

      // Sum up hourly data over entire dataset for each dimension combination
      double total = 0.0;
      Map<String, Number> summedValues = new HashMap<>();

      for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
        double sum = 0.0;
        for (Map.Entry<String, Number[]> hourlyEntry : entry.getValue().entrySet()) {
          double value = hourlyEntry.getValue()[0].doubleValue();
          sum += value;
        }
        summedValues.put(entry.getKey(), sum);
        total += sum;
      }

      // compare by value ascending (want poll to remove smallest element)
      PriorityQueue<Pair<String, Double>> topNValues =
          new PriorityQueue<>(DIMENSION_VALUES_LIMIT, new Comparator<Pair<String, Double>>() {
            @Override
            public int compare(Pair<String, Double> a, Pair<String, Double> b) {
              return Double.compare(a.getValue().doubleValue(), b.getValue().doubleValue());
            }
          });

      double threshold = total * DIMENSION_VALUES_OPTIONS_THRESHOLD;

      // For each dimension value, add it only if it meets the threshold and drop the smallest from
      // the priority queue if over the limit.
      for (Map.Entry<String, Number> entry : summedValues.entrySet()) {
        List<String> combination = objectMapper.readValue(entry.getKey(), LIST_TYPE_REF);
        String dimensionValue = combination.get(i);
        double dimensionValueContribution = entry.getValue().doubleValue();
        if (dimensionValueContribution >= threshold) {
          topNValues.add(new Pair<>(dimensionValue, dimensionValueContribution));
          if (topNValues.size() > DIMENSION_VALUES_LIMIT) {
            Pair<String, Double> removed = topNValues.poll();
          }
        }
      }

      // Poll returns the elements in order of ascending contribution, so poll and reverse the
      // order.
      List<String> sortedValues = new LinkedList<>();
      while (!topNValues.isEmpty()) {
        Pair<String, Double> pair = topNValues.poll();
        sortedValues.add(0, pair.getKey());
      }
      collectedDimensionValues.put(dimension, sortedValues);
    }
    return collectedDimensionValues;
  }
}
