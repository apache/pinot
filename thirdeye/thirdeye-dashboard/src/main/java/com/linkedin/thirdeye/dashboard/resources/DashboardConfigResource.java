package com.linkedin.thirdeye.dashboard.resources;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Future;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * REST controller providing general collection information such as dimensions and metrics.
 * @author jteoh
 */
@Path("/dashboardConfig")
@Produces(MediaType.APPLICATION_JSON)
public class DashboardConfigResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardConfigResource.class);
  private static final String DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION = "AGGREGATE_1_HOURS(%s)";
  private static final String DIMENSION_VALUES_OPTIONS_THRESHOLD = "0.01";
  private static final String DIMENSION_VALUES_LIMIT = "25";
  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };

  private final String serverUri;
  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;

  public DashboardConfigResource(String serverUri, DataCache dataCache, QueryCache queryCache,
      ObjectMapper objectMapper) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
  }

  /** Return a list of dimensions for the provided collection. */
  @GET
  @Path("/dimensions/{collection}")
  public List<String> getDimensions(@PathParam("collection") String collection) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    return schema.getDimensions();
  }

  /**
   * Return a list of dimension aliases for the provided collection. If no alias exists, 'null' will
   * be returned in the JSON response.
   */
  @GET
  @Path("/dimensionAliases/{collection}")
  public List<String> getDimensionAliases(@PathParam("collection") String collection)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    return schema.getDimensionAliases();
  }

  /**
   * Returns a map of dimensions -> list of observed dimension values in the given time
   * window, sorted in decreasing order of overall contribution. Any items not meeting a
   * contribution threshold of {@value #DIMENSION_VALUES_OPTIONS_THRESHOLD} are omitted, and a
   * maximum of {@value #DIMENSION_VALUES_LIMIT} results are returned for any given dimension.
   * </br>
   * Baseline/current are provided to limit the range in which to search for queries.
   */
  @GET
  @Path("/dimensionValues/{collection}/{baseline}/{current}")
  public Map<String, Collection<String>> getDimensionValues(
      @PathParam("collection") String collection, @PathParam("baseline") long baselineMillis,
      @PathParam("current") long currentMillis,
      @DefaultValue(DIMENSION_VALUES_OPTIONS_THRESHOLD) @QueryParam("threshold") double contributionThreshold,
      @DefaultValue(DIMENSION_VALUES_LIMIT) @QueryParam("limit") int dimensionValuesLimit)
          throws Exception {
    return retrieveDimensionValues(collection, baselineMillis, currentMillis, contributionThreshold,
        dimensionValuesLimit);
  }

  /** Return a list of metrics for the provided collection. */
  @GET
  @Path("/metrics/{collection}")
  public List<String> getMetrics(@PathParam("collection") String collection) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    return schema.getMetrics();
  }

  /**
   * Return a list of metric aliases for the provided collection. If no alias exists, 'null' will
   * be returned in the JSON response.
   */
  @GET
  @Path("/metricAliases/{collection}")
  public List<String> getMetricAliases(@PathParam("collection") String collection)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    return schema.getMetricAliases();
  }

  private Map<String, Collection<String>> retrieveDimensionValues(String collection,
      long baselineMillis, long currentMillis, double contributionThreshold,
      int dimensionValuesLimit) throws Exception {
    List<String> dimensions = getDimensions(collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);

    String firstMetric = getMetrics(collection).get(0);
    String dummyFunction = String.format(DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION, firstMetric);

    MultivaluedMap<String, String> dimensionValues = new MultivaluedMapImpl();
    Map<String, Future<QueryResult>> resultFutures = new HashMap<>();
    // query w/ group by for each dimension.
    for (String dimension : dimensions) {
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
          new PriorityQueue<>(dimensionValuesLimit, new Comparator<Pair<String, Double>>() {
            @Override
            public int compare(Pair<String, Double> a, Pair<String, Double> b) {
              return Double.compare(a.getValue().doubleValue(), b.getValue().doubleValue());
            }
          });

      double threshold = total * contributionThreshold;

      // For each dimension value, add it only if it meets the threshold and drop an element from
      // the priority queue if over the limit.
      for (Map.Entry<String, Number> entry : summedValues.entrySet()) {
        List<String> combination = objectMapper.readValue(entry.getKey(), LIST_TYPE_REF);
        String dimensionValue = combination.get(i);
        double dimensionValueContribution = entry.getValue().doubleValue();
        if (dimensionValueContribution >= threshold) {
          topNValues.add(new Pair<>(dimensionValue, dimensionValueContribution));
          if (topNValues.size() > dimensionValuesLimit) {
            topNValues.poll();
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
