package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.FlotTimeSeries;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.util.UriUtils;
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
  private final String serverUri;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;

  public FlotTimeSeriesResource(String serverUri, DataCache dataCache, QueryCache queryCache, ObjectMapper objectMapper) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
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
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    String sql = SqlUtils.getSql(metricFunction, collection, baseline, current, uriInfo.getQueryParameters());
    QueryResult queryResult = queryCache.getQueryResult(serverUri, sql).checkEmpty();
    return FlotTimeSeries.fromQueryResult(schema, objectMapper, queryResult);
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

    // Generate SQL
    String baselineSeriesSql
        = SqlUtils.getSql(metricFunction, collection, baselineRangeStart, baselineRangeEnd, dimensionValues);
    String currentSeriesSql
        = SqlUtils.getSql(metricFunction, collection, currentRangeStart, currentRangeEnd, dimensionValues);

    // Query (async)
    Future<QueryResult> baselineResult
        = queryCache.getQueryResultAsync(serverUri, baselineSeriesSql);
    Future<QueryResult> currentResult
        = queryCache.getQueryResultAsync(serverUri, currentSeriesSql);

    // Generate series
    List<FlotTimeSeries> baselineSeries
        = FlotTimeSeries.fromQueryResult(schema, objectMapper, baselineResult.get().checkEmpty(), BASELINE_LABEL_PREFIX);
    List<FlotTimeSeries> currentSeries
        = FlotTimeSeries.fromQueryResult(schema, objectMapper, currentResult.get().checkEmpty());

    // Shift all baseline results up by window size
    long offsetMillis = currentMillis - baselineMillis;
    for (FlotTimeSeries series : baselineSeries) {
      for (Number[] point : series.getData()) {
        point[0] = point[0].longValue() + offsetMillis;
      }
    }

    // Combine
    List<FlotTimeSeries> combinedSeries = new ArrayList<>(baselineSeries.size() + currentSeries.size());
    combinedSeries.addAll(baselineSeries);
    combinedSeries.addAll(currentSeries);
    return combinedSeries;
  }
}
