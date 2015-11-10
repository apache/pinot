package com.linkedin.thirdeye.dashboard.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Future;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
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
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");

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
  public List<String> getDimensions(@PathParam("collection") String collection,
      @Context UriInfo uriInfo) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    Set<String> selectedDimensions = retrieveSelectedDimensions(uriInfo);
    if (CollectionUtils.isEmpty(selectedDimensions)) {
      return schema.getDimensions();
    }
    List<String> filteredDimensions =
        new ArrayList<>(CollectionUtils.subtract(schema.getDimensions(), selectedDimensions));
    return filteredDimensions;
  }

  /**
   * Return a list of dimension aliases for the provided collection. If no alias exists, 'null' will
   * be returned in the JSON response.
   */
  @GET
  @Path("/dimensionAliases/{collection}")
  public List<String> getDimensionAliases(@PathParam("collection") String collection,
      @Context UriInfo uriInfo) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    Set<String> selectedDimensions = retrieveSelectedDimensions(uriInfo);
    if (CollectionUtils.isEmpty(selectedDimensions)) {
      return schema.getDimensionAliases();
    }
    Iterator<String> dimensionsIter = schema.getDimensions().iterator();
    Iterator<String> dimensionAliasesIter = schema.getDimensionAliases().iterator();
    List<String> filteredAliases = new LinkedList<>();
    while (dimensionsIter.hasNext()) {
      String dimension = dimensionsIter.next();
      String dimensionAlias = dimensionAliasesIter.next();
      if (!selectedDimensions.contains(dimension)) {
        filteredAliases.add(dimensionAlias);
      }
    }
    return filteredAliases;

  }

  /** Returns null if the info object is null. */
  private MultivaluedMap<String, String> retrieveSelectedDimensionValues(UriInfo uriInfo) {
    return (uriInfo == null ? null : uriInfo.getQueryParameters());
  }

  private Set<String> retrieveSelectedDimensions(UriInfo uriInfo) {
    MultivaluedMap<String, String> map = retrieveSelectedDimensionValues(uriInfo);
    return (map == null ? null : map.keySet());
  }

  /**
   * Returns a map of dimensions -> list of observed dimension values in the given time
   * window, sorted in decreasing order of overall contribution. Any items not meeting a
   * contribution threshold of {@value #DIMENSION_VALUES_OPTIONS_THRESHOLD} for at least one metric
   * are omitted, and a
   * maximum of {@value #DIMENSION_VALUES_LIMIT} results are returned for each metric and any given
   * dimension.
   * </br>
   * Baseline/current are provided to limit the range in which to search for dimension values.
   * </br>
   * This method currently does not take into account already selected dimensions.
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

  public Map<String, Collection<String>> getDimensionValues(String collection, long baselineMillis,
      long currentMillis) throws NumberFormatException, Exception {
    return getDimensionValues(collection, baselineMillis, currentMillis,
        Double.valueOf(DIMENSION_VALUES_OPTIONS_THRESHOLD),
        Integer.valueOf(DIMENSION_VALUES_LIMIT));
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
    List<String> dimensions = getDimensions(collection, null);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);

    List<String> metrics = getMetrics(collection);
    String dummyFunction = String.format(DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION,
        METRIC_FUNCTION_JOINER.join(metrics));

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
      int metricCount = metrics.size();
      double[] total = new double[metricCount];
      Map<String, double[]> summedValues = new HashMap<>();

      for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
        double[] sum = new double[metricCount];
        for (Map.Entry<String, Number[]> hourlyEntry : entry.getValue().entrySet()) {
          for (int j = 0; j < metricCount; j++) {
            double value = hourlyEntry.getValue()[j].doubleValue();
            sum[j] += value;
          }
        }
        summedValues.put(entry.getKey(), sum);
        // update total w/ sums for each dimension value.
        for (int j = 0; j < metricCount; j++) {
          total[j] += sum[j];
        }

      }

      // compare by value ascending (want poll to remove smallest element)
      List<PriorityQueue<Pair<String, Double>>> topNValuesByMetric =
          new ArrayList<PriorityQueue<Pair<String, Double>>>(metricCount);
      double[] threshold = new double[metricCount];
      Comparator<Pair<String, Double>> valueComparator = new Comparator<Pair<String, Double>>() {
        @Override
        public int compare(Pair<String, Double> a, Pair<String, Double> b) {
          return Double.compare(a.getValue().doubleValue(), b.getValue().doubleValue());
        }
      };
      for (int j = 0; j < metricCount; j++) {
        threshold[j] = total[j] * contributionThreshold;
        topNValuesByMetric.add(new PriorityQueue<>(dimensionValuesLimit, valueComparator));
      }

      // For each dimension value, add it only if it meets the threshold and drop an element from
      // the priority queue if over the limit.
      for (Map.Entry<String, double[]> entry : summedValues.entrySet()) {
        List<String> combination = objectMapper.readValue(entry.getKey(), LIST_TYPE_REF);
        String dimensionValue = combination.get(i);
        for (int j = 0; j < metricCount; j++) { // metricCount == entry.getValue().length
          double dimensionValueContribution = entry.getValue()[j];
          if (dimensionValueContribution >= threshold[j]) {
            PriorityQueue<Pair<String, Double>> topNValues = topNValuesByMetric.get(j);
            topNValues.add(new Pair<>(dimensionValue, dimensionValueContribution));
            if (topNValues.size() > dimensionValuesLimit) {
              topNValues.poll();
            }
          }
        }
      }

      // Poll returns the elements in order of ascending contribution, so poll and reverse the
      // order.

      // not LinkedHashSet because we need to reverse insertion order with metrics.
      List<String> sortedValues = new ArrayList<>();
      HashSet<String> sortedValuesSet = new HashSet<>();

      for (int j = 0; j < metricCount; j++) {
        PriorityQueue<Pair<String, Double>> topNValues = topNValuesByMetric.get(j);
        int startIndex = sortedValues.size();
        while (!topNValues.isEmpty()) {
          Pair<String, Double> pair = topNValues.poll();
          String dimensionValue = pair.getKey();
          if (!sortedValuesSet.contains(dimensionValue)) {
            sortedValues.add(startIndex, dimensionValue);
            sortedValuesSet.add(dimensionValue);
          }
        }
      }

      collectedDimensionValues.put(dimension, sortedValues);
    }
    return collectedDimensionValues;
  }

}
