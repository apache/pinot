package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.database.AnomalyTable;
import com.linkedin.thirdeye.anomaly.database.AnomalyTableRow;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.FlotTimeSeries;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.*;

import org.joda.time.DateTime;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Path("/flot")
@Produces(MediaType.APPLICATION_JSON)
public class FlotTimeSeriesResource {
  private static final String BASELINE_LABEL_PREFIX = "BASELINE_";
  private static final String ANOMALY_LABEL_PREFIX = "ANOMALY_";
  private final String serverUri;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;
  private final ConfigCache configCache;
  private final AnomalyDatabaseConfig anomalyDatabase;
  private final boolean displayAnomalies;

  public FlotTimeSeriesResource(String serverUri, DataCache dataCache, QueryCache queryCache, ObjectMapper objectMapper,
      ConfigCache configCache, AnomalyDatabaseConfig anomalyDatabase) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
    this.configCache = configCache;
    this.anomalyDatabase = anomalyDatabase;

    displayAnomalies = anomalyDatabase != null;
  }

  @GET
  @Path("/TIME_SERIES_FULL/{collection}/{metricFunction}/{baselineMillis}/{currentMillis}")
  public List<FlotTimeSeries> getAll(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis,
      @Context UriInfo uriInfo) throws Exception {
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);
    // Dimension groups
    Map<String, Map<String, List<String>>> reverseDimensionGroups = null;
    DimensionGroupSpec dimensionGroupSpec = configCache.getDimensionGroupSpec(collection);
    if (dimensionGroupSpec != null) {
      reverseDimensionGroups = dimensionGroupSpec.getReverseMapping();
    }
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    String sql = SqlUtils.getSql(metricFunction, collection, baseline, current, uriInfo.getQueryParameters(), reverseDimensionGroups);
    QueryResult queryResult = queryCache.getQueryResult(serverUri, sql).checkEmpty();

    List<FlotTimeSeries> allSeries = FlotTimeSeries.fromQueryResult(schema, objectMapper, queryResult);
    if (displayAnomalies) {
      List<AnomalyTableRow> anomalies = AnomalyTable.selectRows(anomalyDatabase, collection, null, null, null, null,
          null, false, null, new TimeRange(baselineMillis, currentMillis));
      allSeries.addAll(FlotTimeSeries.anomaliesFromQueryResult(schema, objectMapper, queryResult, ANOMALY_LABEL_PREFIX,
          anomalies));
    }
    return allSeries;
  }

  @GET
  @Path("/TIME_SERIES_OVERLAY/{collection}/{metricFunction}/{baselineMillis}/{currentMillis}/{windowMillis}")
  public List<FlotTimeSeries> getOverlay(
      @PathParam("collection") String collection,
      @PathParam("metricFunction") String metricFunction,
      @PathParam("baselineMillis") Long baselineMillis,
      @PathParam("currentMillis") Long currentMillis,
      @PathParam("windowMillis") Long windowMillis,
      @Context UriInfo uriInfo) throws Exception {
    DateTime baselineRangeStart = new DateTime(baselineMillis - windowMillis);
    DateTime baselineRangeEnd = new DateTime(baselineMillis);
    DateTime currentRangeStart = new DateTime(currentMillis - windowMillis);
    DateTime currentRangeEnd = new DateTime(currentMillis);
    MultivaluedMap<String, String> dimensionValues = uriInfo.getQueryParameters();
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);

    // Dimension groups
    Map<String, Map<String, List<String>>> reverseDimensionGroups = null;
    DimensionGroupSpec dimensionGroupSpec = configCache.getDimensionGroupSpec(collection);
    if (dimensionGroupSpec != null) {
      reverseDimensionGroups = dimensionGroupSpec.getReverseMapping();
    }

    // Generate SQL
    String baselineSeriesSql
        = SqlUtils.getSql(metricFunction, collection, baselineRangeStart, baselineRangeEnd, dimensionValues, reverseDimensionGroups);
    String currentSeriesSql
        = SqlUtils.getSql(metricFunction, collection, currentRangeStart, currentRangeEnd, dimensionValues, reverseDimensionGroups);

    // Query (async)
    Future<QueryResult> baselineResult
        = queryCache.getQueryResultAsync(serverUri, baselineSeriesSql);
    Future<QueryResult> currentResult
        = queryCache.getQueryResultAsync(serverUri, currentSeriesSql);

    // Query for anomalies
    List<AnomalyTableRow> anomalies = null;
    if (displayAnomalies) {
      anomalies = AnomalyTable.selectRows(anomalyDatabase, collection, null, null, null, null, null, false, null,
          new TimeRange(currentMillis - windowMillis, currentMillis));
    }

    // Generate series
    List<FlotTimeSeries> baselineSeries
        = FlotTimeSeries.fromQueryResult(schema, objectMapper, baselineResult.get().checkEmpty(), BASELINE_LABEL_PREFIX);

    QueryResult currentQueryResult = currentResult.get().checkEmpty();
    List<FlotTimeSeries> currentSeries
        = FlotTimeSeries.fromQueryResult(schema, objectMapper, currentQueryResult);

    List<FlotTimeSeries> anomalySeries;
    if (displayAnomalies) {
      anomalySeries = FlotTimeSeries.anomaliesFromQueryResult(schema, objectMapper, currentQueryResult,
          ANOMALY_LABEL_PREFIX, anomalies);
    } else {
      anomalySeries = new ArrayList<>(0);
    }

    // Shift all baseline results up by window size
    long offsetMillis = currentMillis - baselineMillis;
    for (FlotTimeSeries series : baselineSeries) {
      for (Number[] point : series.getData()) {
        point[0] = point[0].longValue() + offsetMillis;
      }
    }

    // Combine
    List<FlotTimeSeries> combinedSeries = new ArrayList<>(baselineSeries.size() + currentSeries.size()
        + anomalySeries.size());
    combinedSeries.addAll(currentSeries);
    combinedSeries.addAll(baselineSeries);
    combinedSeries.addAll(anomalySeries);
    return combinedSeries;
  }
}
